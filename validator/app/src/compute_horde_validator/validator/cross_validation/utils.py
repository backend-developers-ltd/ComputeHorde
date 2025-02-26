import logging

import bittensor
from asgiref.sync import sync_to_async
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.transport import AbstractTransport
from django.core.cache import cache

from compute_horde_validator.validator.models import SystemEvent
from compute_horde_validator.validator.utils import TRUSTED_MINER_FAKE_KEY

logger = logging.getLogger(__name__)


class TrustedMinerClient(OrganicMinerClient):
    def __init__(
        self,
        miner_address: str,
        miner_port: int,
        job_uuid: str,
        my_keypair: bittensor.Keypair,
        transport: AbstractTransport | None = None,
    ) -> None:
        super().__init__(
            miner_hotkey=TRUSTED_MINER_FAKE_KEY,
            miner_address=miner_address,
            miner_port=miner_port,
            job_uuid=job_uuid,
            my_keypair=my_keypair,
            transport=transport,
        )


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
        long_description="",
        data={},
    )
    cache.set("trusted_miner_not_configured_system_event_sent", True, timeout=24 * 60 * 60)
