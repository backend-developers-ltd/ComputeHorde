import logging

from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.protocol_messages import GenericError, UnauthorizedError
from django.conf import settings

from compute_horde_validator.validator.models import SystemEvent

logger = logging.getLogger(__name__)


class MinerClient(OrganicMinerClient):
    async def notify_generic_error(self, msg: GenericError) -> None:
        desc = f"Received error message from miner {self.miner_name}: {msg.model_dump_json()}"
        await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
            type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
            long_description=desc,
            data={},
        )

    async def notify_unauthorized_error(self, msg: UnauthorizedError) -> None:
        desc = f"Unauthorized in {self.miner_name}: {msg.code}, details: {msg.details}"
        await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
            type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.UNAUTHORIZED,
            long_description=desc,
            data={},
        )

    async def notify_receipt_failure(self, comment: str) -> None:
        await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
            type=SystemEvent.EventType.RECEIPT_FAILURE,
            subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
            long_description=comment,
            data={"job_uuid": self.job_uuid, "miner_hotkey": self.miner_hotkey},
        )

    async def notify_send_failure(self, msg: str) -> None:
        await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
            type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.MINER_SEND_ERROR,
            long_description=msg,
            data={"job_uuid": self.job_uuid, "miner_hotkey": self.miner_hotkey},
        )
