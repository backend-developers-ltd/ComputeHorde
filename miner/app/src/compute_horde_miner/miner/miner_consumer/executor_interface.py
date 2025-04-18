import logging
from functools import cached_property

from compute_horde.protocol_messages import (
    ExecutorToMinerMessage,
    GenericError,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0InitialJobRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
    V0JobRequest,
    V0MachineSpecsRequest,
    V0StreamingJobNotReadyRequest,
    V0StreamingJobReadyRequest,
)
from pydantic import TypeAdapter

from compute_horde_miner.miner.miner_consumer.base_compute_horde_consumer import (
    BaseConsumer,
    log_errors_explicitly,
)
from compute_horde_miner.miner.miner_consumer.layer_utils import ExecutorInterfaceMixin
from compute_horde_miner.miner.models import AcceptedJob

logger = logging.getLogger(__name__)


class MinerExecutorConsumer(BaseConsumer[ExecutorToMinerMessage], ExecutorInterfaceMixin):
    # Job is populated only after the connection initialization succeeds
    _maybe_job: AcceptedJob | None = None

    class NotInitialized(Exception):
        pass

    @cached_property
    def job(self) -> AcceptedJob:
        if self._maybe_job is None:
            raise MinerExecutorConsumer.NotInitialized("Missing job")
        return self._maybe_job

    @cached_property
    def executor_token(self):
        return self.scope["url_route"]["kwargs"]["executor_token"]

    @log_errors_explicitly
    async def connect(self) -> None:
        # TODO using advisory locks make sure that only one consumer per executor token exists
        await super().connect()
        try:
            # TODO maybe one day tokens will be reused, then we will have to add filtering here
            self._maybe_job = job = await AcceptedJob.objects.aget(
                executor_token=self.executor_token
            )
        except AcceptedJob.DoesNotExist:
            await self.send(
                GenericError(
                    details=f"No job waiting for token {self.executor_token}"
                ).model_dump_json()
            )
            logger.error(f"No job waiting for token {self.executor_token}")
            await self.websocket_disconnect(
                {"code": f"No job waiting for token {self.executor_token}"}
            )
            return
        if job.status != AcceptedJob.Status.WAITING_FOR_EXECUTOR:
            msg = f"Job with token {self.executor_token} is not waiting for an executor"
            await self.send(GenericError(details=msg).model_dump_json())
            logger.error(msg)
            await self.websocket_disconnect({"code": msg})
            return

        await self.group_add(self.executor_token)

        initial_job_request = V0InitialJobRequest.model_validate(job.initial_job_details)
        if initial_job_request.streaming_details:
            initial_job_request.streaming_details.executor_ip = self.get_executor_ip()
        await self.send(initial_job_request.model_dump_json())

    def get_executor_ip(self) -> str:
        if self.job.executor_address:
            return str(self.job.executor_address)
        # Get the real IP from the headers
        for key, value in self.scope["headers"]:
            if key.decode("utf-8").lower() == "x-real-ip":
                return str(value.decode("utf-8"))
        # Fallback to client's IP if header is not present
        return str(self.scope["client"][0])

    def parse_message(self, raw_msg: str | bytes) -> ExecutorToMinerMessage:
        return TypeAdapter(ExecutorToMinerMessage).validate_json(raw_msg)

    async def handle(self, msg: ExecutorToMinerMessage) -> None:
        if isinstance(msg, V0ExecutorReadyRequest):
            self.job.status = AcceptedJob.Status.WAITING_FOR_PAYLOAD
            await self.job.asave()
            await self.send_executor_ready(self.executor_token, msg)
        if isinstance(msg, V0ExecutorFailedRequest):
            self.job.status = AcceptedJob.Status.FAILED
            await self.job.asave()
            await self.send_executor_failed_to_prepare(self.executor_token, msg)
        if isinstance(msg, V0StreamingJobReadyRequest):
            # Job status is RUNNING
            msg.ip = self.get_executor_ip()
            await self.send_streaming_job_ready(self.executor_token, msg)
        if isinstance(msg, V0StreamingJobNotReadyRequest):
            self.job.status = AcceptedJob.Status.FAILED
            await self.job.asave()
            await self.send_streaming_job_failed_to_prepare(self.executor_token, msg)
        if isinstance(msg, V0JobFinishedRequest):
            self.job.status = AcceptedJob.Status.FINISHED
            self.job.exit_status = msg.return_code
            # TODO: self.job.timed_out = msg.timed_out
            self.job.stderr = msg.docker_process_stderr
            self.job.stdout = msg.docker_process_stdout
            self.job.artifacts = msg.artifacts or {}

            await self.job.asave()
            await self.send_executor_finished(self.executor_token, msg)
        if isinstance(msg, V0MachineSpecsRequest):
            await self.send_executor_specs(self.executor_token, msg)
        if isinstance(msg, V0JobFailedRequest):
            self.job.status = AcceptedJob.Status.FAILED
            self.job.error_type = msg.error_type
            self.job.error_detail = msg.error_detail

            await self.job.asave()
            await self.send_executor_failed(self.executor_token, msg)

    async def _miner_job_request(self, msg: V0JobRequest) -> None:
        await self.send(msg.model_dump_json())

    async def disconnect(self, close_code) -> None:
        logger.info(f"Executor {self.executor_token} disconnected")
