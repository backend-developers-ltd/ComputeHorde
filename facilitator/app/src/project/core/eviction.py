import importlib
import logging
from datetime import timedelta

from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from django.conf import settings
from django.utils.timezone import now

from project.core.models import (
    GPU,
    CpuSpecs,
    HardwareState,
    Job,
    MinerVersion,
    OtherSpecs,
    RawSpecsData,
    RawSpecsSnapshot,
)

RECEIPTS_RETENTION_PERIOD = timedelta(days=7)
JOBS_RETENTION_PERIOD = timedelta(days=7)
MINER_VERSION_RETENTION_PERIOD = timedelta(days=30)
MACHINE_SPECS_RETENTION_PERIOD = timedelta(days=7)

logger = logging.getLogger(__name__)


def evict_all() -> None:
    evict_receipts()
    evict_jobs()
    evict_miner_versions()
    evict_machine_specs()
    evict_from_additional_apps()


def evict_receipts() -> None:
    logger.info("Evicting old receipts")
    cutoff = now() - RECEIPTS_RETENTION_PERIOD
    JobStartedReceipt.objects.filter(timestamp__lt=cutoff).delete()
    JobAcceptedReceipt.objects.filter(timestamp__lt=cutoff).delete()
    JobFinishedReceipt.objects.filter(timestamp__lt=cutoff).delete()


def evict_jobs() -> None:
    logger.info("Evicting old jobs")
    cutoff = now() - JOBS_RETENTION_PERIOD
    Job.objects.filter(created_at__lt=cutoff).delete()


def evict_miner_versions() -> None:
    logger.info("Evicting old miner versions")
    cutoff = now() - MINER_VERSION_RETENTION_PERIOD
    MinerVersion.objects.filter(created_at__lt=cutoff).delete()


def evict_machine_specs() -> None:
    logger.info("Evicting old machine specs")
    cutoff = now() - MACHINE_SPECS_RETENTION_PERIOD

    # Will also cascade delete:
    #   RawSpecsData -> ExecutorSpecsSnapshot
    #   RawSpecsData -> ParsedSpecsData -> GpuSpecs
    RawSpecsData.objects.filter(created_at__lt=cutoff).delete()

    HardwareState.objects.filter(measured_at__lt=cutoff).delete()

    # Will also cascade delete:
    #   GPU -> GpuCount
    #   GPU -> GpuSpecs
    GPU.objects.filter(created_at__lt=cutoff).delete()

    # Kill those that no one loves anymore
    CpuSpecs.objects.filter(cpu_specs__isnull=True).delete()
    OtherSpecs.objects.filter(other_specs__isnull=True).delete()

    # TODO: check if it is ok to drop RawSpecsSnapshot altogether
    RawSpecsSnapshot.objects.filter(measured_at__lt=cutoff).delete()


def evict_from_additional_apps():
    """
    Evict old data from additional apps.

    Additional apps can implement their own eviction the following way:
        - Add a module `eviction` at the root of the app
        - In the `eviction` module add a function:
            def evict(retention_period: timedelta = _DEFAULT) -> None: ...
    """
    for app in settings.ADDITIONAL_APPS:
        try:
            mod = importlib.import_module(f"{app}.eviction")
            mod.evict()
        except (ImportError, AttributeError):
            pass
