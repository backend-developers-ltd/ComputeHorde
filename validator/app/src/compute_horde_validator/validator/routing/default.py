import logging
from typing import assert_never

from asgiref.sync import sync_to_async
from compute_horde.blockchain.block_cache import aget_current_block
from compute_horde.fv_protocol.facilitator_requests import (
    OrganicJobRequest,
    V2JobRequest,
)
from compute_horde.utils import async_synchronized
from django.conf import settings

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.types import (
    CannotReserveAllowanceException,
    NotEnoughAllowanceException,
)
from compute_horde_validator.validator.allowance.types import (
    Miner as AllowanceMiner,
)
from compute_horde_validator.validator.models import (
    MetagraphSnapshot,
    Miner,
)
from compute_horde_validator.validator.routing.base import RoutingBase
from compute_horde_validator.validator.routing.types import (
    AllMinersBusy,
    JobRoute,
)
from compute_horde_validator.validator.utils import TRUSTED_MINER_FAKE_KEY

logger = logging.getLogger(__name__)


class Routing(RoutingBase):
    async def pick_miner_for_job_request(self, request: OrganicJobRequest) -> JobRoute:
        if isinstance(request, V2JobRequest):
            return await _pick_miner_for_job_v2(request)

        assert_never(request)


_routing_instance: Routing | None = None


def routing() -> Routing:
    global _routing_instance
    if _routing_instance is None:
        _routing_instance = Routing()
    return _routing_instance


@async_synchronized
async def _pick_miner_for_job_v2(request: V2JobRequest) -> JobRoute:
    executor_class = request.executor_class
    logger.info(f"Picking a miner for job {request.uuid} with executor class {executor_class}")

    if settings.DEBUG_MINER_KEY:
        logger.debug(f"Using DEBUG_MINER_KEY for job {request.uuid}")
        miner_model, _ = await Miner.objects.aget_or_create(hotkey=settings.DEBUG_MINER_KEY)
        miner = AllowanceMiner(
            address=miner_model.address,
            port=miner_model.port,
            ip_version=miner_model.ip_version,
            hotkey_ss58=miner_model.hotkey,
        )
        return JobRoute(miner=miner, allowance_reservation_id=None)

    if request.on_trusted_miner:
        logger.debug(f"Using TRUSTED_MINER for job {request.uuid}")
        miner_model, _ = await Miner.objects.aget_or_create(hotkey=TRUSTED_MINER_FAKE_KEY)
        miner = AllowanceMiner(
            address=miner_model.address,
            port=miner_model.port,
            ip_version=miner_model.ip_version,
            hotkey_ss58=miner_model.hotkey,
        )
        return JobRoute(miner=miner, allowance_reservation_id=None)

    # Calculate total executor-seconds required for the job
    executor_seconds = (
        request.download_time_limit + request.execution_time_limit + request.upload_time_limit
    )

    try:
        current_block = (await MetagraphSnapshot.aget_latest()).block
    except Exception as exc:
        logger.warning(f"Failed to get latest metagraph snapshot: {exc}")
        current_block = await aget_current_block()

    # 1. Find miners with enough allowance
    try:
        suitable_miners = await sync_to_async(allowance().find_miners_with_allowance)(
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

    miners = {miner.hotkey_ss58: miner for miner in await sync_to_async(allowance().miners)()}

    # 2. Iterate and try to reserve a miner
    for miner_hotkey, _ in suitable_miners:
        try:
            # 3. Reserve allowance for this miner
            reservation_id, _ = await sync_to_async(allowance().reserve_allowance)(
                miner=miner_hotkey,
                executor_class=executor_class,
                allowance_seconds=executor_seconds,
                job_start_block=current_block,
            )

            miner = miners[miner_hotkey]
            await Miner.objects.aget_or_create(
                address=miner.address,
                ip_version=miner.ip_version,
                port=miner.port,
                hotkey=miner.hotkey_ss58,
            )
            logger.info(
                f"Successfully reserved miner {miner_hotkey} for job {request.uuid} with reservation ID {reservation_id}"
            )
            return JobRoute(miner=miner, allowance_reservation_id=reservation_id)

        except CannotReserveAllowanceException:
            logger.debug(
                f"Failed to reserve miner {miner_hotkey} for job {request.uuid}, trying next one."
            )
            continue  # Try the next miner in the list

    # If the loop completes without returning, all suitable miners failed to be reserved
    logger.error(f"All suitable miners were busy or failed to reserve for job {request.uuid}.")
    raise AllMinersBusy("Could not reserve any of the suitable miners.")
