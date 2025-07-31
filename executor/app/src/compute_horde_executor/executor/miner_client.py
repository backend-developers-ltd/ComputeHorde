import asyncio
import logging
from dataclasses import dataclass

import pydantic
from compute_horde import organic_job_errors, protocol_consts
from compute_horde.miner_client.base import AbstractMinerClient, UnsupportedMessageReceived
from compute_horde.protocol_messages import (
    ExecutorToMinerMessage,
    GenericError,
    MinerToExecutorMessage,
    V0ExecutionDoneRequest,
    V0ExecutorReadyRequest,
    V0HordeFailedRequest,
    V0InitialJobRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
    V0JobRequest,
    V0MachineSpecsRequest,
    V0StreamingJobReadyRequest,
    V0VolumesReadyRequest,
)
from compute_horde.transport import AbstractTransport, WSTransport
from compute_horde.utils import MachineSpecs
from django.conf import settings
from pydantic import TypeAdapter

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    """Exit output and status of the job's docker container."""

    timed_out: bool
    """Whether the job's docker container timed out and was forced to exit."""

    return_code: int | None
    """Exit code returned by the job process. None means the job timed out and was stopped."""

    stdout: str
    stderr: str


class JobError(Exception):
    def __init__(
        self,
        error_message: str,
        error_type: protocol_consts.JobFailureReason | None = None,
        error_detail: str | None = None,
        execution_result: ExecutionResult | None = None,
    ):
        self.error_message = error_message
        self.error_type = error_type
        self.error_detail = error_detail
        self.execution_result = execution_result


class MinerClient(AbstractMinerClient[MinerToExecutorMessage, ExecutorToMinerMessage]):
    class NotInitialized(Exception):
        pass

    def __init__(self, miner_address: str, token: str, transport: AbstractTransport | None = None):
        self.miner_address = miner_address
        self.token = token
        transport = transport or WSTransport(miner_address, self.miner_url())
        super().__init__(miner_address, transport)

        self._maybe_job_uuid: str | None = None
        loop = asyncio.get_running_loop()
        self.initial_msg: asyncio.Future[V0InitialJobRequest] = loop.create_future()
        self.initial_msg_lock = asyncio.Lock()
        self.full_payload: asyncio.Future[V0JobRequest] = loop.create_future()
        self.full_payload_lock = asyncio.Lock()

    @property
    def job_uuid(self) -> str:
        if self._maybe_job_uuid is None:
            raise MinerClient.NotInitialized("Job UUID is missing")
        return self._maybe_job_uuid

    def miner_url(self) -> str:
        return f"{self.miner_address}/v0.1/executor_interface/{self.token}"

    def parse_message(self, raw_msg: str | bytes) -> MinerToExecutorMessage:
        return TypeAdapter(MinerToExecutorMessage).validate_json(raw_msg)

    async def handle_message(self, msg: MinerToExecutorMessage) -> None:
        if isinstance(msg, V0InitialJobRequest):
            await self.handle_initial_job_request(msg)
        elif isinstance(msg, V0JobRequest):
            await self.handle_job_request(msg)
        else:
            raise UnsupportedMessageReceived(msg)

    async def handle_initial_job_request(self, msg: V0InitialJobRequest):
        async with self.initial_msg_lock:
            if self.initial_msg.done():
                details = f"Received duplicate initial job request: first {self.job_uuid=} and then {msg.job_uuid=}"
                logger.error(details)
                await self.send_generic_error(details)
                return
            self._maybe_job_uuid = msg.job_uuid
            logger.debug(f"Received initial job request: {msg.job_uuid=}")
            self.initial_msg.set_result(msg)

    async def handle_job_request(self, msg: V0JobRequest):
        async with self.full_payload_lock:
            if not self.initial_msg.done():
                details = f"Received job request before an initial job request {msg.job_uuid=}"
                logger.error(details)
                await self.send_generic_error(details)
                return
            if self.full_payload.done():
                details = (
                    f"Received duplicate full job payload request: first "
                    f"{self.job_uuid=} and then {msg.job_uuid=}"
                )
                logger.error(details)
                await self.send_generic_error(details)
                return
            logger.debug(f"Received full job payload request: {msg.job_uuid=}")
            self.full_payload.set_result(msg)

    async def send_streaming_job_ready(self, certificate: str):
        await self.send_model(
            V0StreamingJobReadyRequest(
                job_uuid=self.job_uuid,
                public_key=certificate,
                port=settings.NGINX_PORT,
            )
        )

    async def send_executor_ready(self):
        await self.send_model(V0ExecutorReadyRequest(job_uuid=self.job_uuid))

    async def send_volumes_ready(self):
        await self.send_model(V0VolumesReadyRequest(job_uuid=self.job_uuid))

    async def send_execution_done(self):
        await self.send_model(V0ExecutionDoneRequest(job_uuid=self.job_uuid))

    async def old_send_job_error(self, job_error: JobError):
        # TODO(post error propagation): get rid of this method and use send_job_failed or send_horde_failed
        message = job_error.error_message
        if job_error.error_detail:
            message += f": {job_error.error_detail}"

        if job_error.execution_result:
            await self.send_job_failed(
                organic_job_errors.JobFailed(
                    stage=protocol_consts.JobStage.UNKNOWN,
                    reason=protocol_consts.JobFailureReason.NONZERO_EXIT_CODE,
                    message=message,
                    docker_process_exit_status=job_error.execution_result.return_code,
                    docker_process_stdout=job_error.execution_result.stdout,
                    docker_process_stderr=job_error.execution_result.stderr,
                )
            )

        else:
            await self.send_horde_failed(
                organic_job_errors.HordeFailed(
                    reported_by=protocol_consts.JobParticipantType.EXECUTOR,
                    stage=protocol_consts.JobStage.UNKNOWN,
                    reason=protocol_consts.HordeFailureReason.GENERIC_EXECUTOR_FAILED,
                    message=job_error.error_message,
                )
            )

    async def send_job_failed(self, job_failure: organic_job_errors.JobFailed):
        await self.send_model(
            V0JobFailedRequest(
                job_uuid=self.job_uuid,
                stage=job_failure.stage,
                reason=job_failure.reason,
                message=job_failure.message,
                context=job_failure.context,
                docker_process_exit_status=job_failure.docker_process_exit_status,
                docker_process_stdout=job_failure.docker_process_stdout,
                docker_process_stderr=job_failure.docker_process_stderr,
            )
        )

    async def send_horde_failed(self, horde_failure: organic_job_errors.HordeFailed):
        await self.send_model(
            V0HordeFailedRequest(
                job_uuid=self.job_uuid,
                reported_by=horde_failure.reported_by,
                stage=horde_failure.stage,
                reason=horde_failure.reason,
                message=horde_failure.message,
                context=horde_failure.context,
            )
        )

    async def send_result(self, job_result: "JobResult"):
        if job_result.specs:
            await self.send_model(
                V0MachineSpecsRequest(
                    job_uuid=self.job_uuid,
                    specs=job_result.specs,
                )
            )
        await self.send_model(
            V0JobFinishedRequest(
                job_uuid=self.job_uuid,
                docker_process_stdout=job_result.stdout,
                docker_process_stderr=job_result.stderr,
                artifacts=job_result.artifacts,
                upload_results=job_result.upload_results,
            )
        )

    async def send_generic_error(self, details: str):
        await self.send_model(
            GenericError(
                details=details,
            )
        )


class JobResult(pydantic.BaseModel):
    exit_status: int | None
    timeout: bool
    stdout: str
    stderr: str
    artifacts: dict[str, str]
    specs: MachineSpecs | None = None
    error_type: protocol_consts.JobFailureReason | None = None
    error_detail: str | None = None
    upload_results: dict[str, str]
