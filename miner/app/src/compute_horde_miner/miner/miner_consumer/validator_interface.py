import logging
import uuid

from compute_horde.mv_protocol import miner_requests, validator_requests
from compute_horde.mv_protocol.validator_requests import BaseValidatorRequest

from compute_horde_miner.miner.executor_manager import current
from compute_horde_miner.miner.executor_manager.base import ExecutorUnavailable
from compute_horde_miner.miner.miner_consumer.base_compute_horde_consumer import (
    BaseConsumer,
    log_errors_explicitly,
)
from compute_horde_miner.miner.miner_consumer.layer_utils import (
    ExecutorFailed,
    ExecutorFailedToPrepare,
    ExecutorFinished,
    ExecutorReady,
    ValidatorInterfaceMixin,
)
from compute_horde_miner.miner.models import AcceptedJob, Validator

logger = logging.getLogger(__name__)


class MinerValidatorConsumer(BaseConsumer, ValidatorInterfaceMixin):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.validator_key = ''
        self.validator: Validator | None = None
        self.pending_jobs: dict[str, 'AcceptedJob'] = {}

    @log_errors_explicitly
    async def connect(self):
        await super().connect()
        # TODO verify ssl cert
        self.validator_key = self.scope['url_route']['kwargs']['validator_key']
        self.validator = (await Validator.objects.aget_or_create(public_key=self.validator_key))[0]
        self.pending_jobs = await AcceptedJob.get_for_validator(self.validator)
        for job in self.pending_jobs.values():
            await self.group_add(job.executor_token)
            if job.status != AcceptedJob.Status.WAITING_FOR_PAYLOAD:
                continue
            await self.send(miner_requests.V0ExecutorReadyRequest(job_uuid=job.job_uuid).model_dump_json())
            logger.debug(f'Readiness for job {job.job_uuid} reported to validator {self.validator_key}')

        for job in (await AcceptedJob.get_not_reported(self.validator)):
            if job.status == AcceptedJob.Status.FINISHED:
                await self.send(miner_requests.V0JobFinishedRequest(
                    job_uuid=str(job.job_uuid),
                    docker_process_stdout=job.stdout,
                    docker_process_stderr=job.stderr,
                ).model_dump_json())
                logger.debug(f'Job {job.job_uuid} finished reported to validator {self.validator_key}')
            else:  # job.status == AcceptedJob.Status.FAILED:
                await self.send(miner_requests.V0JobFailedRequest(
                    job_uuid=str(job.job_uuid),
                    docker_process_stdout=job.stdout,
                    docker_process_stderr=job.stderr,
                    docker_process_exit_status=job.exit_status,
                ).model_dump_json())
                logger.debug(f'Failed job {job.job_uuid} reported to validator {self.validator_key}')
            job.result_reported_to_validator = True
            await job.asave()
        # TODO using advisory locks make sure that only one consumer per validator exists

    def accepted_request_type(self):
        return BaseValidatorRequest

    def incoming_generic_error_class(self):
        return validator_requests.GenericError

    def outgoing_generic_error_class(self):
        return miner_requests.GenericError

    async def handle(self, msg: BaseValidatorRequest):
        if isinstance(msg, validator_requests.V0InitialJobRequest):
            # TODO add rate limiting per validator key here
            token = f'{msg.job_uuid}-{uuid.uuid4()}'
            await self.group_add(token)
            job = AcceptedJob(
                validator=self.validator,
                job_uuid=msg.job_uuid,
                executor_token=token,
                initial_job_details=msg.model_dump(),
                status=AcceptedJob.Status.WAITING_FOR_EXECUTOR,
            )
            await job.asave()
            self.pending_jobs[msg.job_uuid] = job

            try:
                current.executor_manager.reserve_executor(token)
            except ExecutorUnavailable:
                await self.send(miner_requests.V0DeclineJobRequest(job_uuid=msg.job_uuid).model_dump_json())
                await self.group_discard(token)
                await job.adelete()
                self.pending_jobs.pop(msg.job_uuid)
                return
            await self.send(miner_requests.V0AcceptJobRequest(job_uuid=msg.job_uuid).model_dump_json())

        if isinstance(msg, validator_requests.V0JobRequest):
            job = self.pending_jobs.get(msg.job_uuid)
            if job is None:
                logger.error(f"Received JobRequest for unknown job_uuid: {msg.job_uuid}")
                await self.send(miner_requests.GenericError(
                    details=f"Received JobRequest for unknown job_uuid: {msg.job_uuid}").model_dump_json())
                return
            await self.send_job_request(job.executor_token, msg)
            logger.debug(f"Passing job details to executor consumer job_uuid: {msg.job_uuid}")
            job.status = AcceptedJob.Status.RUNNING
            job.full_job_details = msg.model_dump()
            await job.asave()

    async def _executor_ready(self, msg: ExecutorReady):
        job = await AcceptedJob.objects.aget(executor_token=msg.executor_token)
        self.pending_jobs[job.job_uuid] = job
        await self.send(miner_requests.V0ExecutorReadyRequest(job_uuid=str(job.job_uuid)).model_dump_json())
        logger.debug(f'Readiness for job {job.job_uuid} reported to validator {self.validator_key}')

    async def _executor_failed_to_prepare(self, msg: ExecutorFailedToPrepare):
        jobs = [job for job in self.pending_jobs.values() if job.executor_token == msg.executor_token]
        if not jobs or jobs[0].status != AcceptedJob.Status.WAITING_FOR_EXECUTOR:
            return
        job = jobs[0]
        self.pending_jobs = {k: v for k, v in self.pending_jobs.items() if v.executor_token == msg.executor_token}
        await self.send(miner_requests.V0ExecutorFailedRequest(job_uuid=job.job_uuid).model_dump_json())
        logger.debug(f'Failure in preparation for job {job.job_uuid} reported to validator {self.validator_key}')

    async def _executor_finished(self, msg: ExecutorFinished):
        await self.send(miner_requests.V0JobFinishedRequest(
            job_uuid=msg.job_uuid,
            docker_process_stdout=msg.docker_process_stdout,
            docker_process_stderr=msg.docker_process_stderr,
        ).model_dump_json())
        logger.debug(f'Finished job {msg.job_uuid} reported to validator {self.validator_key}')
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = True
        await job.asave()

    async def _executor_failed(self, msg: ExecutorFailed):
        await self.send(miner_requests.V0JobFailedRequest(
            job_uuid=msg.job_uuid,
            docker_process_stdout=msg.docker_process_stdout,
            docker_process_stderr=msg.docker_process_stderr,
            docker_process_exit_status=msg.docker_process_exit_status,
        ).model_dump_json())
        logger.debug(f'Failed job {msg.job_uuid} reported to validator {self.validator_key}')
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = True
        await job.asave()

    async def disconnect(self, close_code):
        logger.info(f'Validator {self.validator_key} disconnected')
