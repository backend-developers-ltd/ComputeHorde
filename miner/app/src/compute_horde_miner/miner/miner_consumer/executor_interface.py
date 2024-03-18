import logging

from compute_horde.em_protocol import executor_requests, miner_requests
from compute_horde.em_protocol.executor_requests import BaseExecutorRequest
from compute_horde.mv_protocol import validator_requests

from compute_horde_miner.miner.miner_consumer.base_compute_horde_consumer import (
    BaseConsumer,
    log_errors_explicitly,
)
from compute_horde_miner.miner.miner_consumer.layer_utils import ExecutorInterfaceMixin, JobRequest
from compute_horde_miner.miner.miner_consumer.validator_interface import executor_semaphore
from compute_horde_miner.miner.models import AcceptedJob

logger = logging.getLogger(__name__)


class MinerExecutorConsumer(BaseConsumer, ExecutorInterfaceMixin):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.executor_token = ''
        self.job: AcceptedJob | None = None

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
        self.executor_token = self.scope['url_route']['kwargs']['executor_token']
        try:
            # TODO maybe one day tokens will be reused, then we will have to add filtering here
            job = await AcceptedJob.objects.aget(executor_token=self.executor_token)
        except AcceptedJob.DoesNotExist:
            await self.send(miner_requests.GenericError(
                details=f'No job waiting for token {self.executor_token}').json())
            logger.error(f'No job waiting for token {self.executor_token}')
            await self.websocket_disconnect({"code": f'No job waiting for token {self.executor_token}'})
            return
        if job.status != AcceptedJob.Status.WAITING_FOR_EXECUTOR:
            msg = f'Job with token {self.executor_token} is not waiting for an executor'
            await self.send(miner_requests.GenericError(details=msg).json())
            logger.error(msg)
            await self.websocket_disconnect(
                {"code": msg})
            return

        self.job = job
        await self.group_add(self.executor_token)
        initial_job_details = validator_requests.V0InitialJobRequest(**job.initial_job_details)
        await self.send(miner_requests.V0InitialJobRequest(
            job_uuid=initial_job_details.job_uuid,
            base_docker_image_name=initial_job_details.base_docker_image_name,
            timeout_seconds=initial_job_details.timeout_seconds,
            volume_type=initial_job_details.volume_type.value,
        ).json())

    async def handle(self, msg: BaseExecutorRequest):
        if isinstance(msg, executor_requests.V0ReadyRequest):
            self.job.status = AcceptedJob.Status.WAITING_FOR_PAYLOAD
            await self.job.asave()
            await self.send_executor_ready(self.executor_token)
        if isinstance(msg, executor_requests.V0FailedToPrepare):
            self.job.status = AcceptedJob.Status.FAILED
            await self.job.asave()
            await self.send_executor_failed_to_prepare(self.executor_token)
        if isinstance(msg, executor_requests.V0FinishedRequest):
            self.job.status = AcceptedJob.Status.FINISHED
            self.job.stderr = msg.docker_process_stderr
            self.job.stdout = msg.docker_process_stdout

            await self.job.asave()
            await self.send_executor_finished(
                job_uuid=msg.job_uuid,
                executor_token=self.executor_token,
                stdout=msg.docker_process_stdout,
                stderr=msg.docker_process_stderr
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
        await self.send(miner_requests.V0JobRequest(
            job_uuid=msg.job_uuid,
            docker_image_name=msg.docker_image_name,
            raw_script=msg.raw_script,
            docker_run_options_preset=msg.docker_run_options_preset,
            docker_run_cmd=msg.docker_run_cmd,
            volume=msg.volume,
            output_upload=msg.output_upload,
        ).json())

    async def disconnect(self, close_code):
        logger.info(f'Executor {self.executor_token} disconnected')
        executor_semaphore.release()
