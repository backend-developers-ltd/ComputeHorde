from datetime import timedelta

from django.utils.timezone import now

from compute_horde_validator.validator.models import SystemEvent


async def trusted_miner_not_configured_system_event(type_: SystemEvent.EventType) -> None:
    """
    Create a system event with (type_ / TRUSTED_MINER_NOT_CONFIGURED), if such
    an event was not created in the last 24 hours.
    """
    exists_in_24h = await SystemEvent.objects.filter(
        type=type_,
        subtype=SystemEvent.EventSubType.TRUSTED_MINER_NOT_CONFIGURED,
        timestamp__gt=now() - timedelta(hours=24),
    ).aexists()
    if exists_in_24h:
        return

    await SystemEvent.objects.acreate(
        type=type_,
        subtype=SystemEvent.EventSubType.TRUSTED_MINER_NOT_CONFIGURED,
        timestamp=now(),
        long_description="",
        data={},
    )
