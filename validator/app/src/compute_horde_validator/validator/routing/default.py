import asyncio
import logging
from datetime import datetime
from typing import assert_never

from asgiref.sync import async_to_sync, sync_to_async
from compute_horde.fv_protocol.facilitator_requests import (
    OrganicJobRequest,
    V2JobRequest,
)
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings
from django.utils import timezone

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.types import (
    CannotReserveAllowanceException,
    NotEnoughAllowanceException,
    blocks_ids,
    reservation_id,
)
from compute_horde_validator.validator.allowance.types import Miner as AllowanceMiner
from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.receipts.default import receipts
from compute_horde_validator.validator.routing.base import JobRouteBase, RoutingBase
from compute_horde_validator.validator.routing.types import AllMinersBusy
from compute_horde_validator.validator.utils import TRUSTED_MINER_FAKE_KEY

logger = logging.getLogger(__name__)


class JobRoute(JobRouteBase):
    def __init__(
        self, miner: Miner, allowance_blocks: blocks_ids, allowance_reservation_id: reservation_id
    ):
        super().__init__(miner=miner)
        self.allowance_blocks = allowance_blocks
        self.allowance_reservation_id = allowance_reservation_id


class Routing(RoutingBase):
    def __init__(self):
        self._lock = asyncio.Lock()

    async def pick_miner_for_job_request(self, request: OrganicJobRequest) -> JobRouteBase:
        if isinstance(request, V2JobRequest):
            async with self._lock:
                return await sync_to_async(_pick_miner_for_job_v2)(request)

        assert_never(request)


_routing_instance: Routing | None = None


def routing() -> Routing:
    global _routing_instance
    if _routing_instance is None:
        _routing_instance = Routing()
    return _routing_instance


def _pick_miner_for_job_v2(request: V2JobRequest) -> JobRouteBase:
    executor_class = request.executor_class
    logger.info(f"Picking a miner for job {request.uuid} with executor class {executor_class}")

    if settings.DEBUG_MINER_KEY:
        logger.debug(f"Using DEBUG_MINER_KEY for job {request.uuid}")
        miner_model, _ = Miner.objects.get_or_create(hotkey=settings.DEBUG_MINER_KEY)
        miner = AllowanceMiner(
            address=miner_model.address,
            port=miner_model.port,
            ip_version=miner_model.ip_version,
            hotkey_ss58=miner_model.hotkey,
        )
        # FIXME: implement `allowance_blocks` reservation, it must not be None
        return JobRoute(miner=miner, allowance_blocks=[], allowance_reservation_id=None)

    if request.on_trusted_miner:
        logger.debug(f"Using TRUSTED_MINER for job {request.uuid}")
        miner_model, _ = Miner.objects.get_or_create(hotkey=TRUSTED_MINER_FAKE_KEY)
        miner = AllowanceMiner(
            address=miner_model.address,
            port=miner_model.port,
            ip_version=miner_model.ip_version,
            hotkey_ss58=miner_model.hotkey,
        )
        return JobRoute(miner=miner, allowance_blocks=None, allowance_reservation_id=None)

    # Calculate total executor-seconds required for the job
    executor_seconds = (
        request.download_time_limit + request.execution_time_limit + request.upload_time_limit
    )

    current_block = allowance().get_current_block()

    # 1. Find miners with enough allowance
    try:
        suitable_miners = allowance().find_miners_with_allowance(
            allowance_seconds=executor_seconds,
            executor_class=executor_class,
            job_start_block=current_block,
        )
        assert suitable_miners
    except NotEnoughAllowanceException as e:
        logger.warning(
            f"Could not find any miners with enough allowance for job {request.uuid}: {e}"
        )
        raise

    miners = {miner.hotkey_ss58: miner for miner in allowance().miners()}
    manifests = allowance().get_manifests()

    # Get receipts instance once to avoid bound method issues with async_to_sync
    receipts_instance = receipts()

    # Create async wrapper functions to avoid bound method warnings
    async def get_busy_executor_count(executor_class: ExecutorClass, at_time: datetime):
        return await receipts_instance.get_busy_executor_count(executor_class, at_time)

    busy_executors = async_to_sync(get_busy_executor_count)(executor_class, timezone.now())

    # 2. Iterate and try to reserve a miner
    for miner_hotkey, _ in suitable_miners:
        ongoing_jobs = busy_executors.get(miner_hotkey, 0)

        executor_dict = manifests.get(miner_hotkey, {})
        executor_count = executor_dict.get(executor_class, 0)

        if ongoing_jobs >= executor_count:
            logger.debug(
                f"Skipping miner {miner_hotkey} with {executor_count} executors "
                f"{ongoing_jobs} ongoing jobs at [{current_block}]"
            )
            continue

        logger.info(
            f"Picking miner {miner_hotkey} with {executor_count} executors "
            f"{ongoing_jobs} ongoing jobs at [{current_block}]"
        )

        try:
            # 3. Reserve allowance for this miner
            reservation_id, blocks = allowance().reserve_allowance(
                miner=miner_hotkey,
                executor_class=executor_class,
                allowance_seconds=executor_seconds,
                job_start_block=current_block,
            )

            miner = miners[miner_hotkey]
            Miner.objects.get_or_create(
                address=miner.address,
                ip_version=miner.ip_version,
                port=miner.port,
                hotkey=miner.hotkey_ss58,
            )
            logger.info(
                f"Successfully reserved miner {miner_hotkey} for job {request.uuid} with reservation ID {reservation_id}"
            )
            return JobRoute(
                miner=miner, allowance_blocks=blocks, allowance_reservation_id=reservation_id
            )

        except CannotReserveAllowanceException:
            logger.debug(
                f"Failed to reserve miner {miner_hotkey} for job {request.uuid}, trying next one."
            )
            continue  # Try the next miner in the list

    # If the loop completes without returning, all suitable miners failed to be reserved
    logger.error(f"All suitable miners were busy or failed to reserve for job {request.uuid}.")
    raise AllMinersBusy("Could not reserve any of the suitable miners.")
