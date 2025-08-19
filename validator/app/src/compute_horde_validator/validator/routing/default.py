import logging
import math
from datetime import timedelta
from typing import assert_never

from compute_horde.blockchain.block_cache import aget_current_block
from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import (
    OrganicJobRequest,
    V2JobRequest,
)
from compute_horde.subtensor import get_cycle_containing_block
from compute_horde.utils import async_synchronized
from django.conf import settings
from django.utils import timezone

from compute_horde_validator.validator import collateral
from compute_horde_validator.validator.allowance.types import Miner as AllowanceMiner
from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import (
    ComputeTimeAllowance,
    MetagraphSnapshot,
    Miner,
    MinerManifest,
    MinerPreliminaryReservation,
)
from compute_horde_validator.validator.receipts.default import Receipts
from compute_horde_validator.validator.routing.base import RoutingBase
from compute_horde_validator.validator.routing.types import (
    AllMinersBusy,
    JobRoute,
    NoMinerForExecutorType,
    NoMinerWithEnoughAllowance,
    NotEnoughTimeInCycle,
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


def _get_seconds_remaining_in_current_cycle(current_block: int) -> int:
    cycle = get_cycle_containing_block(current_block, netuid=settings.BITTENSOR_NETUID)
    time_remaining_in_cycle = (
        cycle.stop - current_block
    ) * settings.BITTENSOR_APPROXIMATE_BLOCK_DURATION
    return math.floor(time_remaining_in_cycle.total_seconds())


@async_synchronized
async def _pick_miner_for_job_v2(request: V2JobRequest) -> JobRoute:
    """
    Goes through all miners with recent manifests and online executors of the given executor class.
    Filters miners based on compute time allowance and minimum collateral requirements.
    Creates a preliminary reservation for the selected miner and returns the miner with:
    - Highest percentage of remaining allowance
    - Highest collateral as a tiebreaker
    - Available executors (less ongoing jobs than online executor count)
    - Sufficient remaining time in the current cycle
    """

    executor_class = request.executor_class
    logger.info(f"Picking a miner for job {request.uuid} with executor class {executor_class}")

    if settings.DEBUG_MINER_KEY:
        logger.debug(f"Using DEBUG_MINER_KEY for job {request.uuid}")
        miner, _ = await Miner.objects.aget_or_create(hotkey=settings.DEBUG_MINER_KEY)
        return JobRoute(
            miner=AllowanceMiner(
                address=miner.address,
                ip_version=miner.ip_version,
                port=miner.port,
                hotkey_ss58=miner.hotkey,
            ),
            allowance_reservation_id=None,
        )

    if request.on_trusted_miner:
        logger.debug(f"Using trusted miner for job {request.uuid}")
        miner, _ = await Miner.objects.aget_or_create(hotkey=TRUSTED_MINER_FAKE_KEY)
        return JobRoute(
            miner=AllowanceMiner(
                address=miner.address,
                ip_version=miner.ip_version,
                port=miner.port,
                hotkey_ss58=miner.hotkey,
            ),
            allowance_reservation_id=None,
        )

    executor_seconds = (
        request.download_time_limit + request.execution_time_limit + request.upload_time_limit
    )

    block: int | None = None
    try:
        block = (await MetagraphSnapshot.aget_latest()).block
    except Exception as exc:
        logger.warning(f"Failed to get latest metagraph snapshot: {exc}")
        block = await aget_current_block()
    assert block is not None, "Failed to get current block from cache or subtensor."

    if not await aget_config("DYNAMIC_ALLOW_CROSS_CYCLE_ORGANIC_JOBS"):
        seconds_remaining_in_cycle = _get_seconds_remaining_in_current_cycle(block)

        seconds_required_in_cycle = (
            await aget_config("DYNAMIC_ORGANIC_JOB_ALLOWED_LEEWAY_TIME")
            + await aget_config("DYNAMIC_EXECUTOR_RESERVATION_TIME_LIMIT")
            + await aget_config("DYNAMIC_EXECUTOR_STARTUP_TIME_LIMIT")
            + EXECUTOR_CLASS[executor_class].spin_up_time
            + executor_seconds
        )

        if seconds_remaining_in_cycle < seconds_required_in_cycle:
            logger.debug(
                f"NotEnoughTimeInCycle: {seconds_remaining_in_cycle=} {seconds_required_in_cycle=}"
            )
            raise NotEnoughTimeInCycle(seconds_remaining_in_cycle, seconds_required_in_cycle)

    manifests_qs = (
        MinerManifest.objects.select_related("miner")
        .filter(
            miner__in=Miner.objects.non_blacklisted(),
            executor_class=str(executor_class),
            created_at__gte=timezone.now() - timedelta(hours=4),
        )
        .order_by("created_at")
    )
    manifests = [manifest async for manifest in manifests_qs.all()]

    # Only accept the latest manifest for each miner
    latest_miner_manifest: dict[str, MinerManifest] = {}
    for manifest in manifests:
        latest_miner_manifest[manifest.miner.hotkey] = manifest

    for manifest in latest_miner_manifest.values():
        logger.debug(
            f"Latest {manifest.miner.hotkey} manifest has {manifest.online_executor_count} executors of type {executor_class}"
        )

    # Discard manifests that explicitly say there are no executors of this type
    latest_miner_manifest = {
        hotkey: manifest
        for hotkey, manifest in latest_miner_manifest.items()
        if manifest.online_executor_count > 0
    }

    if not latest_miner_manifest:
        logger.error(f"Failed to find a miner with available executors of type {executor_class}")
        raise NoMinerForExecutorType(executor_class)

    # filter/sort miners based on available allowance
    allowance_qs = ComputeTimeAllowance.objects.select_related("miner").filter(
        cycle__start__lte=block,
        cycle__stop__gt=block,
        miner__hotkey__in=latest_miner_manifest.keys(),
        validator__hotkey=settings.BITTENSOR_WALLET().hotkey.ss58_address,
    )
    if await aget_config("DYNAMIC_CHECK_ALLOWANCE_WHILE_ROUTING"):
        allowance_qs = allowance_qs.filter(remaining_allowance__gt=executor_seconds)
    else:
        logger.warning("Allowance check disabled with dynamic config.")

    allowances = [allowance async for allowance in allowance_qs.all()]

    if not allowances:
        raise NoMinerWithEnoughAllowance()

    # prefer miners with more allowance
    allowances.sort(
        key=lambda allowance: (
            # percentage of remaining allowance
            (allowance.remaining_allowance / allowance.initial_allowance)
            if allowance.initial_allowance
            else 0.0,
            # miner collateral as a tiebreaker
            allowance.miner.collateral_wei,
        ),
        reverse=True,
    )

    minimum_collateral = await aget_config("DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI")
    contract_address = await collateral.get_collateral_contract_address_async()

    for allowance in allowances:
        miner = allowance.miner
        manifest = latest_miner_manifest[miner.hotkey]
        if contract_address and int(miner.collateral_wei) < minimum_collateral:
            logger.warning(
                f"Miner {manifest.miner.hotkey} has {int(miner.collateral_wei)} collateral, but required minimum is {minimum_collateral}"
            )
            continue

        preliminary_reservation_jobs: set[str] = {
            str(job_uuid)
            async for job_uuid in MinerPreliminaryReservation.objects.active()
            .filter(
                miner=miner,
                executor_class=str(executor_class),
            )
            .values_list("job_uuid", flat=True)
        }

        started_receipts = await Receipts().get_valid_job_started_receipts_for_miner(
            miner.hotkey, timezone.now()
        )
        known_started_jobs: set[str] = {str(receipt.job_uuid) for receipt in started_receipts}

        finished_receipts = await Receipts().get_job_finished_receipts_for_miner(
            miner.hotkey, list(known_started_jobs | preliminary_reservation_jobs)
        )
        known_finished_jobs: set[str] = {str(receipt.job_uuid) for receipt in finished_receipts}

        maybe_ongoing_jobs = (
            preliminary_reservation_jobs | known_started_jobs
        ) - known_finished_jobs

        logger.debug(
            f"Miner {manifest.miner.hotkey} has "
            f"{len(preliminary_reservation_jobs)} preliminary reservations, "
            f"{len(known_started_jobs)} known started, "
            f"{len(known_finished_jobs)} known finished, "
            f"{len(maybe_ongoing_jobs)} maybe ongoing jobs"
        )

        if len(maybe_ongoing_jobs) < manifest.online_executor_count:
            reservation_time = await aget_config(
                "DYNAMIC_ROUTING_PRELIMINARY_RESERVATION_TIME_SECONDS"
            )
            await MinerPreliminaryReservation.objects.acreate(
                miner=miner,
                executor_class=executor_class,
                job_uuid=request.uuid,
                expires_at=timezone.now() + timedelta(seconds=reservation_time),
            )
            logger.info(f"Picked miner {manifest.miner.hotkey}")
            return JobRoute(
                miner=AllowanceMiner(
                    address=miner.address,
                    ip_version=miner.ip_version,
                    port=miner.port,
                    hotkey_ss58=miner.hotkey,
                ),
                allowance_reservation_id=None,
            )

    logger.error("All miners are busy")
    raise AllMinersBusy()
