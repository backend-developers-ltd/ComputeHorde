import logging

from asgiref.sync import sync_to_async
from django.core.cache import cache
from django.utils.timezone import now

from compute_horde_validator.validator.models import SystemEvent

logger = logging.getLogger(__name__)


@sync_to_async
def trusted_miner_not_configured_system_event(type_: SystemEvent.EventType) -> None:
    """
    Create a system event with (type_ / TRUSTED_MINER_NOT_CONFIGURED), if such
    an event was not created in the last 24 hours.
    """
    if cache.get("trusted_miner_not_configured_system_event_sent"):
        logger.warning("skipping TRUSTED_MINER_NOT_CONFIGURED system event, already exists in 24h")
        return

    SystemEvent.objects.create(
        type=type_,
        subtype=SystemEvent.EventSubType.TRUSTED_MINER_NOT_CONFIGURED,
        timestamp=now(),
        long_description="",
        data={},
    )
    cache.set("trusted_miner_not_configured_system_event_sent", True, timeout=24 * 60 * 60)
