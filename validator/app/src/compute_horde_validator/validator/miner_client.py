import asyncio
import datetime
import logging
import time
import uuid
from functools import partial

import bittensor
from compute_horde.base_requests import BaseRequest
from compute_horde.executor_class import ExecutorClass
from compute_horde.miner_client.base import (
    AbstractMinerClient,
    UnsupportedMessageReceived,
)
from compute_horde.mv_protocol import miner_requests, validator_requests
from compute_horde.mv_protocol.miner_requests import (
    BaseMinerRequest,
    ExecutorManifest,
    UnauthorizedError,
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorManifestRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
    V0MachineSpecsRequest,
)
from compute_horde.mv_protocol.validator_requests import (
    AuthenticationPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    V0AuthenticateRequest,
    V0JobFinishedReceiptRequest,
    V0JobStartedReceiptRequest,
)
from compute_horde.transport import AbstractTransport, WSTransport
from compute_horde.utils import MachineSpecs
from django.conf import settings

from compute_horde_validator.validator.models import (
    JobBase,
    Miner,
    MinerManifest,
    SystemEvent,
)

logger = logging.getLogger(__name__)


async def save_job_execution_event(subtype: str, long_description: str, data={}, success=False):
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.MINER_ORGANIC_JOB_SUCCESS
        if success
        else SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


class JobState:
    def __init__(self):
        loop = asyncio.get_running_loop()
        self.miner_ready_or_declining_future = loop.create_future()
        self.miner_ready_or_declining_timestamp: int = 0
        self.miner_finished_or_failed_future = loop.create_future()
        self.miner_finished_or_failed_timestamp: int = 0
        self.miner_machine_specs: MachineSpecs | None = None


class MinerClient(AbstractMinerClient):
    def __init__(
        self,
        miner_address: str,
        my_hotkey: str,
        miner_hotkey: str,
        miner_port: int,
        job_uuid: None | str | uuid.UUID,
        batch_id: None | int,
        keypair: bittensor.Keypair,
        transport: AbstractTransport | None = None,
    ):
        self.miner_hotkey = miner_hotkey
        self.my_hotkey = my_hotkey
        self.miner_address = miner_address
        self.miner_port = miner_port

        name = f"{miner_hotkey}({miner_address}:{miner_port})"
        transport = transport or WSTransport(name, self.miner_url())

        super().__init__(name, transport)

        self.job_states = {}
        if job_uuid is not None:
            self.add_job(job_uuid)
        self.batch_id = batch_id
        self.keypair = keypair
        self._barrier = None
        loop = asyncio.get_running_loop()
        self.miner_manifest = loop.create_future()
        self.online_executor_count = 0

    def add_job(self, job_uuid: str | uuid.UUID):
        job_state = JobState()
        self.job_states[str(job_uuid)] = job_state
        return job_state

    def get_job_state(self, job_uuid: str | uuid.UUID):
        return self.job_states.get(str(job_uuid))

    def get_barrier(self):
        if self._barrier is None:
            self._barrier = asyncio.Barrier(len(self.job_states))
        return self._barrier

    def miner_url(self) -> str:
        return (
            f"ws://{self.miner_address}:{self.miner_port}/v0.1/validator_interface/{self.my_hotkey}"
        )

    def accepted_request_type(self) -> type[BaseRequest]:
        return BaseMinerRequest

    def incoming_generic_error_class(self):
        return miner_requests.GenericError

    def outgoing_generic_error_class(self):
        return validator_requests.GenericError

    async def handle_message(self, msg: BaseRequest):
        if isinstance(msg, self.incoming_generic_error_class()):
            msg = f"Received error message from miner {self.miner_name}: {msg.model_dump_json()}"
            logger.warning(msg)
            await save_job_execution_event(
                subtype=SystemEvent.EventSubType.GENERIC_ERROR, long_description=msg
            )
            return
        elif isinstance(msg, UnauthorizedError):
            logger.error(f"Unauthorized in {self.miner_name}: {msg.code}, details: {msg.details}")
            await save_job_execution_event(
                subtype=SystemEvent.EventSubType.UNAUTHORIZED, long_description=msg
            )
            return
        elif isinstance(msg, V0ExecutorManifestRequest):
            try:
                self.miner_manifest.set_result(msg.manifest)
            except asyncio.InvalidStateError:
                logger.warning(f"Received manifest from {msg} but future was already set")
            return

        job_state = self.get_job_state(msg.job_uuid)
        if job_state is None:
            logger.info(f"Received info about another job: {msg}")
            return

        if isinstance(msg, V0AcceptJobRequest):
            logger.info(f"Miner {self.miner_name} accepted job")
        elif isinstance(
            msg, V0DeclineJobRequest | V0ExecutorFailedRequest | V0ExecutorReadyRequest
        ):
            try:
                job_state.miner_ready_or_declining_future.set_result(msg)
                job_state.miner_ready_or_declining_timestamp = time.time()
            except asyncio.InvalidStateError:
                logger.warning(f"Received {msg} from {self.miner_name} but future was already set")
        elif isinstance(msg, V0JobFailedRequest | V0JobFinishedRequest):
            try:
                job_state.miner_finished_or_failed_future.set_result(msg)
                job_state.miner_finished_or_failed_timestamp = time.time()
            except asyncio.InvalidStateError:
                logger.warning(f"Received {msg} from {self.miner_name} but future was already set")
        elif isinstance(msg, V0MachineSpecsRequest):
            job_state.miner_machine_specs = msg.specs
        else:
            raise UnsupportedMessageReceived(msg)

    async def save_manifest(self, manifest: ExecutorManifest):
        miner = await Miner.objects.aget(hotkey=self.miner_hotkey)
        if self.batch_id:
            await MinerManifest.objects.acreate(
                miner=miner,
                batch_id=self.batch_id,
                executor_count=manifest.total_count,
                online_executor_count=self.online_executor_count,
            )

    def generate_authentication_message(self):
        payload = AuthenticationPayload(
            validator_hotkey=self.my_hotkey,
            miner_hotkey=self.miner_hotkey,
            timestamp=int(time.time()),
        )
        return V0AuthenticateRequest(
            payload=payload, signature=f"0x{self.keypair.sign(payload.blob_for_signing()).hex()}"
        )

    def generate_job_started_receipt_message(
        self, job: JobBase, accepted_timestamp: float, max_timeout: int
    ) -> V0JobStartedReceiptRequest:
        time_accepted = datetime.datetime.fromtimestamp(accepted_timestamp, datetime.UTC)
        receipt_payload = JobStartedReceiptPayload(
            job_uuid=str(job.job_uuid),
            miner_hotkey=job.miner.hotkey,
            validator_hotkey=self.my_hotkey,
            executor_class=ExecutorClass(job.executor_class),
            time_accepted=time_accepted,
            max_timeout=max_timeout,
        )
        return V0JobStartedReceiptRequest(
            payload=receipt_payload,
            signature=f"0x{self.keypair.sign(receipt_payload.blob_for_signing()).hex()}",
        )

    async def send_job_started_receipt_message(
        self, job: JobBase, accepted_timestamp: float, max_timeout: int
    ):
        try:
            receipt_message = self.generate_job_started_receipt_message(
                job,
                accepted_timestamp,
                max_timeout,
            )
            await self.send_model(
                receipt_message, error_event_callback=self.get_job_error_event_callback(job)
            )
            logger.debug(f"Sent job started receipt for {job.job_uuid}")
        except Exception as e:
            comment = f"Failed to send job started receipt to miner {self.miner_name} for job {job.job_uuid}: {e}"
            logger.warning(comment)
            await self.save_receipt_event(
                job=job,
                subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
                long_description=comment,
            )

    def generate_job_finished_receipt_message(
        self, job: JobBase, started_timestamp: float, time_took_seconds: float, score: float
    ) -> V0JobFinishedReceiptRequest:
        time_started = datetime.datetime.fromtimestamp(started_timestamp, datetime.UTC)
        receipt_payload = JobFinishedReceiptPayload(
            job_uuid=str(job.job_uuid),
            miner_hotkey=job.miner.hotkey,
            validator_hotkey=self.my_hotkey,
            time_started=time_started,
            time_took_us=int(time_took_seconds * 1_000_000),
            score_str=f"{score:.6f}",
        )
        return V0JobFinishedReceiptRequest(
            payload=receipt_payload,
            signature=f"0x{self.keypair.sign(receipt_payload.blob_for_signing()).hex()}",
        )

    async def send_job_finished_receipt_message(
        self, job: JobBase, started_timestamp: float, time_took_seconds: float, score: float
    ):
        # Send job finished receipt to miner
        try:
            receipt_message = self.generate_job_finished_receipt_message(
                job, started_timestamp, time_took_seconds, score
            )
            await self.send_model(
                receipt_message, error_event_callback=self.get_job_error_event_callback(job)
            )
            logger.debug(f"Sent job finished receipt for {job.job_uuid}")
        except Exception as e:
            comment = f"Failed to send job finished receipt to miner {self.miner_name} for job {job.job_uuid}: {e}"
            logger.warning(comment)
            await self.save_receipt_event(
                job=job,
                subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
                long_description=comment,
            )

    def get_job_error_event_callback(self, job: JobBase):
        data = {"job_uuid": str(job.job_uuid), "miner_hotkey": job.miner.hotkey}
        save_event = partial(save_job_execution_event, data=data)

        async def handle_send_error_event(msg: str):
            await save_event(
                subtype=SystemEvent.EventSubType.MINER_SEND_ERROR, long_description=msg
            )

        return handle_send_error_event

    async def save_receipt_event(job: JobBase, subtype: str, long_description: str):
        data = {"job_uuid": str(job.job_uuid), "miner_hotkey": job.miner.hotkey}
        SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
            type=SystemEvent.EventType.RECEIPT_FAILURE,
            subtype=subtype,
            long_description=long_description,
            data=data,
        )

    async def connect(self):
        await super().connect()
        await self.transport.send(self.generate_authentication_message().model_dump_json())
