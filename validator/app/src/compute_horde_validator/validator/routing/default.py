import logging
import threading
from datetime import timedelta
from typing import assert_never

from compute_horde.fv_protocol.facilitator_requests import (
    OrganicJobRequest,
    V2JobRequest,
)
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings
from django.db.models import Count
from django.utils import timezone

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.types import (
    CannotReserveAllowanceException,
    NotEnoughAllowanceException,
)
from compute_horde_validator.validator.allowance.types import (
    Miner as AllowanceMiner,
)
from compute_horde_validator.validator.collateral.default import collateral
from compute_horde_validator.validator.dynamic_config import get_config
from compute_horde_validator.validator.models import Miner, MinerIncident, SystemEvent
from compute_horde_validator.validator.receipts.default import receipts
from compute_horde_validator.validator.routing.base import RoutingBase
from compute_horde_validator.validator.routing.metrics import VALIDATOR_MINER_INCIDENT_REPORTED
from compute_horde_validator.validator.routing.types import (
    AllMinersBusy,
    JobRoute,
    MinerIncidentType,
    NotEnoughCollateralException,
)
from compute_horde_validator.validator.routing.utils import weighted_shuffle
from compute_horde_validator.validator.utils import TRUSTED_MINER_FAKE_KEY

logger = logging.getLogger(__name__)


class Routing(RoutingBase):
    def __init__(self):
        self._lock = threading.Lock()

    def pick_miner_for_job_request(self, request: OrganicJobRequest) -> JobRoute:
        if isinstance(request, V2JobRequest):
            with self._lock:
                return _pick_miner_for_job_v2(request)

        assert_never(request)

    def report_miner_incident(
        self,
        type: MinerIncidentType,
        hotkey_ss58address: str,
        job_uuid: str,
        executor_class: ExecutorClass,
    ) -> None:
        MinerIncident.objects.create(
            type=type,
            hotkey_ss58address=hotkey_ss58address,
            job_uuid=job_uuid,
            executor_class=executor_class.value,
        )
        VALIDATOR_MINER_INCIDENT_REPORTED.labels(
            incident_type=type.value,
            miner_hotkey=hotkey_ss58address,
            executor_class=executor_class.value,
        ).inc()


_routing_instance: Routing | None = None


def routing() -> Routing:
    global _routing_instance
    if _routing_instance is None:
        _routing_instance = Routing()
    return _routing_instance


def _pick_miner_for_job_v2(request: V2JobRequest) -> JobRoute:
    executor_class = request.executor_class
    logger.info(f"Picking a miner for job {request.uuid} with executor class {executor_class}")
    logger.info(f"Routing checkpoint {request.uuid}: ENTER")

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
        return JobRoute(
            miner=miner,
            allowance_blocks=[],
            allowance_reservation_id=None,
            allowance_job_value=None,
        )

    if request.on_trusted_miner:
        logger.debug(f"Using TRUSTED_MINER for job {request.uuid}")
        miner_model, _ = Miner.objects.get_or_create(hotkey=TRUSTED_MINER_FAKE_KEY)
        miner = AllowanceMiner(
            address=miner_model.address,
            port=miner_model.port,
            ip_version=miner_model.ip_version,
            hotkey_ss58=miner_model.hotkey,
        )
        return JobRoute(
            miner=miner,
            allowance_blocks=None,
            allowance_reservation_id=None,
            allowance_job_value=None,
        )

    # Calculate total executor-seconds required for the job
    executor_seconds = (
        request.download_time_limit + request.execution_time_limit + request.upload_time_limit
    )

    logger.info(f"Routing checkpoint {request.uuid}: allowance().get_current_block()")
    current_block = allowance().get_current_block()

    # Find miners with enough allowance
    try:
        logger.info(f"Routing checkpoint {request.uuid}: allowance().find_miners_with_allowance()")
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

    # Collateral filtering: build a set of miners with sufficient collateral if threshold > 0
    collateral_threshold = get_config("DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI")
    if collateral_threshold > 0:
        try:
            logger.info(
                f"Routing checkpoint {request.uuid}: collateral().list_miners_with_sufficient_collateral()"
            )
            eligible_collateral_hotkeys = {
                mc.hotkey
                for mc in collateral().list_miners_with_sufficient_collateral(collateral_threshold)
            }
        except Exception as e:
            logger.error(
                "Collateral listing failed, proceeding without collateral filtering: %s", e
            )
        else:
            before_count = len(suitable_miners)
            suitable_miners = [
                (hk, allowance_seconds)
                for hk, allowance_seconds in suitable_miners
                if hk in eligible_collateral_hotkeys
            ]
            if not suitable_miners:
                logger.warning(
                    "All %d miners with allowance filtered out due to insufficient collateral (< %d Wei) for job %s",
                    before_count,
                    collateral_threshold,
                    request.uuid,
                )
                raise NotEnoughCollateralException

    logger.info(f"Routing checkpoint {request.uuid}: allowance().miners()")
    miners = {miner.hotkey_ss58: miner for miner in allowance().miners()}
    manifests = allowance().get_manifests()

    logger.info(f"Routing checkpoint {request.uuid}: _get_miners_reliability_score()")
    reliability_score_per_hotkey = _get_miners_reliability_score(
        reliability_window=timedelta(
            hours=float(get_config("DYNAMIC_ROUTING_RELIABILITY_WINDOW_HOURS"))
        ),
        executor_class=executor_class,
        manifests=manifests,
    )

    suitable_hotkeys = [hotkey for hotkey, _ in suitable_miners]
    hotkey_weights = [reliability_score_per_hotkey.get(hk, 0) for hk in suitable_hotkeys]

    # Default score of 0 - meaning no wrongdoings if we have no data.
    # Note on score values:
    # - any score lower than the cutoff, no matter how low, is only slightly worse than the cutoff value.
    # - similarly, any amount over 0 is only slightly better than 0.
    logger.info(f"Routing checkpoint {request.uuid}: weighted_shuffle()")
    prioritized_hotkeys, probs = weighted_shuffle(
        items=suitable_hotkeys,
        weights=hotkey_weights,
        # With steepness >5 there is a strong cutoff at ~center*2, hence center~=cutoff/2
        center=float(get_config("DYNAMIC_ROUTING_RELIABILITY_SOFT_CUTOFF")) / 2,
        steepness=float(get_config("DYNAMIC_ROUTING_RELIABILITY_SEPARATION")),
    )

    # Get receipts instance once to avoid bound method issues with async_to_sync
    receipts_instance = receipts()

    logger.info(f"Routing checkpoint {request.uuid}: receipts().get_busy_executor_count()")
    busy_executors = receipts_instance.get_busy_executor_count(executor_class, timezone.now())

    system_event = SystemEvent(
        type=SystemEvent.EventType.JOB_ROUTING,
        long_description=f"Job {request.uuid} routing report",
        data={
            "job_uuid": request.uuid,
            "executor_class": executor_class.value,
            "current_block": current_block,
            "executor_seconds": executor_seconds,
            "collateral_threshold": collateral_threshold,
            "reliability_scores": reliability_score_per_hotkey,
            "pick_probabilities": dict(zip(prioritized_hotkeys, probs)),
            "manifests": manifests,
            "busy_executors": busy_executors,
            "skipped_miners": {},  # Hotkey: reason; Filled in later
        },
    )

    # Iterate and try to reserve a miner
    logger.info(f"Routing checkpoint {request.uuid}: for miner_hotkey in prioritized_hotkeys")
    for miner_hotkey in prioritized_hotkeys:
        ongoing_jobs = busy_executors.get(miner_hotkey, 0)

        executor_dict = manifests.get(miner_hotkey, {})
        executor_count = executor_dict.get(executor_class, 0)

        if ongoing_jobs >= executor_count:
            logger.debug(
                f"Skipping miner {miner_hotkey} with {executor_count} executors "
                f"{ongoing_jobs} ongoing jobs at [{current_block}]"
            )
            system_event.data["skipped_miners"][miner_hotkey] = "busy"
            continue

        logger.info(
            f"Picking miner {miner_hotkey} with {executor_count} executors "
            f"{ongoing_jobs} ongoing jobs at [{current_block}]"
        )

        try:
            # Reserve allowance for this miner
            logger.info(
                f"Routing checkpoint {request.uuid}: allowance().reserve_allowance() for {miner_hotkey}"
            )
            reservation_id, blocks = allowance().reserve_allowance(
                miner=miner_hotkey,
                executor_class=executor_class,
                allowance_seconds=executor_seconds,
                job_start_block=current_block,
            )

            miner = miners[miner_hotkey]
            logger.info(
                f"Routing checkpoint {request.uuid}: Miner.objects.update_or_create() for {miner_hotkey}"
            )
            Miner.objects.update_or_create(
                hotkey=miner.hotkey_ss58,
                defaults={
                    "address": miner.address,
                    "ip_version": miner.ip_version,
                    "port": miner.port,
                },
            )
            logger.info(
                f"Successfully reserved miner {miner_hotkey} for job {request.uuid} with reservation ID {reservation_id}"
            )
            system_event.subtype = SystemEvent.EventSubType.JOB_ROUTING_SUCCESS
            system_event.data["picked_miner"] = miner_hotkey
            system_event.save()
            logger.info(f"Routing checkpoint {request.uuid}: LEAVE")
            return JobRoute(
                miner=miner,
                allowance_blocks=blocks,
                allowance_reservation_id=reservation_id,
                allowance_job_value=executor_seconds,
            )

        except CannotReserveAllowanceException:
            logger.debug(
                f"Failed to reserve miner {miner_hotkey} for job {request.uuid}, trying next one."
            )
            system_event.data["skipped_miners"][miner_hotkey] = "cannot reserve allowance"
            continue  # Try the next miner in the list

    # If the loop completes without returning, all suitable miners failed to be reserved
    logger.warning(f"All suitable miners were busy or failed to reserve for job {request.uuid}.")
    system_event.subtype = SystemEvent.EventSubType.JOB_ROUTING_FAILURE
    system_event.save()
    raise AllMinersBusy("Could not reserve any of the suitable miners.")


def _get_miners_reliability_score(
    reliability_window: timedelta,
    executor_class: ExecutorClass,
    manifests: dict[str, dict[ExecutorClass, int]],
) -> dict[str, float]:
    """Return per-executor reliability score for miners.

    Raw reliability is negative incident count (0 for no incidents). This is divided by executor count
    (from manifests) to yield a per-executor reliability value. Higher (closer to 0) is better.
    If executor count is missing or zero, it's treated as 1 to avoid division by zero.
    Miners with no incidents are given score 0.
    """
    reliability_window_start = timezone.now() - reliability_window

    incidents = (
        MinerIncident.objects.filter(
            timestamp__gte=reliability_window_start, executor_class=executor_class.value
        )
        .values("hotkey_ss58address")
        .annotate(cnt=Count("id"))
    )
    raw_scores: dict[str, int] = {row["hotkey_ss58address"]: -row["cnt"] for row in incidents}

    per_executor: dict[str, float] = {}
    for miner_hotkey, execs in manifests.items():
        executor_count = execs.get(executor_class, 1) or 1
        raw = raw_scores.get(miner_hotkey, 0)
        per_executor[miner_hotkey] = raw / executor_count
    return per_executor
