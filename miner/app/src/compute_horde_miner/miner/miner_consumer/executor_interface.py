import logging
from functools import cached_property

from compute_horde.em_protocol import executor_requests, miner_requests
from compute_horde.em_protocol.executor_requests import BaseExecutorRequest
from compute_horde.mv_protocol import validator_requests

from compute_horde_miner.miner.miner_consumer.base_compute_horde_consumer import (
    BaseConsumer,
    log_errors_explicitly,
)
from compute_horde_miner.miner.miner_consumer.layer_utils import ExecutorInterfaceMixin, JobRequest
from compute_horde_miner.miner.models import AcceptedJob

logger = logging.getLogger(__name__)


class MinerExecutorConsumer(BaseConsumer, ExecutorInterfaceMixin):
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

    def accepted_request_type(self):
        return BaseExecutorRequest

    def incoming_generic_error_class(self):
        return executor_requests.GenericError

    def outgoing_generic_error_class(self):
        return miner_requests.GenericError

    @log_errors_explicitly
    async def connect(self):
        # TODO using advisory locks make sure that only one consumer per executor token exists
        await super().connect()
        try:
            # TODO maybe one day tokens will be reused, then we will have to add filtering here
            self._maybe_job = job = await AcceptedJob.objects.aget(
                executor_token=self.executor_token
            )
        except AcceptedJob.DoesNotExist:
            await self.send(
                miner_requests.GenericError(
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
            await self.send(miner_requests.GenericError(details=msg).model_dump_json())
            logger.error(msg)
            await self.websocket_disconnect({"code": msg})
            return

        await self.group_add(self.executor_token)

        request_type = job.initial_job_details.get("message_type", None)
        if request_type == validator_requests.RequestType.V0InitialJobRequest.value:
            request = validator_requests.V0InitialJobRequest(**job.initial_job_details)
            miner_initial_job_request = miner_requests.V0InitialJobRequest(
                job_uuid=request.job_uuid,
                base_docker_image_name=request.base_docker_image_name,
                timeout_seconds=request.timeout_seconds,
                volume=request.volume,
                volume_type=request.volume_type.value if request.volume_type else None,
            )
        elif request_type == validator_requests.RequestType.V1InitialJobRequest.value:
            request = validator_requests.V1InitialJobRequest(**job.initial_job_details)
            miner_initial_job_request = miner_requests.V1InitialJobRequest(
                job_uuid=request.job_uuid,
                base_docker_image_name=request.base_docker_image_name,
                timeout_seconds=request.timeout_seconds,
                volume=request.volume,
                volume_type=request.volume_type.value if request.volume_type else None,
                public_key=request.public_key,
                executor_ip=self.get_executor_ip(),
            )
        else:
            raise ValueError(f"Unknown job message type {request_type}")

        await self.send(miner_initial_job_request.model_dump_json())

    def get_executor_ip(self) -> str:
        if self.job.executor_address:
            return str(self.job.executor_address)
        # Get the real IP from the headers
        for key, value in self.scope["headers"]:
            if key.decode("utf-8").lower() == "x-real-ip":
                return str(value.decode("utf-8"))
        # Fallback to client's IP if header is not present
        return str(self.scope["client"][0])

    async def handle(self, msg: BaseExecutorRequest):
        if isinstance(msg, executor_requests.V0ReadyRequest):
            self.job.status = AcceptedJob.Status.WAITING_FOR_PAYLOAD
            await self.job.asave()
            await self.send_executor_ready(self.executor_token)
        if isinstance(msg, executor_requests.V0FailedToPrepare):
            self.job.status = AcceptedJob.Status.FAILED
            await self.job.asave()
            await self.send_executor_failed_to_prepare(self.executor_token)
        if isinstance(msg, executor_requests.V0StreamingJobReadyRequest):
            # Job status is RUNNING
            await self.send_streaming_job_ready(
                self.executor_token,
                public_key=msg.public_key,
                ip=self.get_executor_ip(),
                port=msg.port,
            )
        if isinstance(msg, executor_requests.V0StreamingJobFailedToPrepareRequest):
            self.job.status = AcceptedJob.Status.FAILED
            await self.job.asave()
            await self.send_streaming_job_failed_to_prepare(self.executor_token)
        if isinstance(msg, executor_requests.V0FinishedRequest):
            self.job.status = AcceptedJob.Status.FINISHED
            self.job.stderr = msg.docker_process_stderr
            self.job.stdout = msg.docker_process_stdout
            self.job.artifacts = msg.artifacts or {}

            await self.job.asave()
            await self.send_executor_finished(
                job_uuid=msg.job_uuid,
                executor_token=self.executor_token,
                stdout=msg.docker_process_stdout,
                stderr=msg.docker_process_stderr,
                artifacts=msg.artifacts,
            )
        if isinstance(msg, executor_requests.V0MachineSpecsRequest):
            await self.send_executor_specs(
                executor_token=self.executor_token, job_uuid=msg.job_uuid, specs=msg.specs
            )
        if isinstance(msg, executor_requests.V0FailedRequest):
            self.job.status = AcceptedJob.Status.FAILED
            self.job.stderr = msg.docker_process_stderr
            self.job.stdout = msg.docker_process_stdout
            self.job.exit_status = msg.docker_process_exit_status

            await self.job.asave()
            await self.send_executor_failed(
                job_uuid=msg.job_uuid,
                executor_token=self.executor_token,
                stdout=msg.docker_process_stdout,
                stderr=msg.docker_process_stderr,
                exit_status=msg.docker_process_exit_status,
            )

    async def _miner_job_request(self, msg: JobRequest):
        await self.send(
            miner_requests.V0JobRequest(
                job_uuid=msg.job_uuid,
                docker_image_name=msg.docker_image_name,
                raw_script=msg.raw_script,
                docker_run_options_preset=msg.docker_run_options_preset,
                docker_run_cmd=msg.docker_run_cmd,
                volume=msg.volume,
                output_upload=msg.output_upload,
                artifacts_dir=msg.artifacts_dir,
            ).model_dump_json()
        )

    async def disconnect(self, close_code):
        logger.info(f"Executor {self.executor_token} disconnected")
