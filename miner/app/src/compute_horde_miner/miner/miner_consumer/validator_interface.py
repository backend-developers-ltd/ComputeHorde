import datetime as dt
import logging
import time
import uuid

import bittensor
from compute_horde.mv_protocol import miner_requests, validator_requests
from compute_horde.mv_protocol.validator_requests import BaseValidatorRequest
from django.conf import settings
from django.utils import timezone

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
from compute_horde_miner.miner.models import (
    AcceptedJob,
    JobFinishedReceipt,
    JobStartedReceipt,
    Validator,
    ValidatorBlacklist,
)
from compute_horde_miner.miner.tasks import prepare_receipts

logger = logging.getLogger(__name__)

AUTH_MESSAGE_MAX_AGE = 10

DONT_CHECK = "DONT_CHECK"


def get_miner_signature(msg: BaseValidatorRequest) -> str:
    keypair = settings.BITTENSOR_WALLET().get_hotkey()
    return f"0x{keypair.sign(msg.blob_for_signing()).hex()}"


class MinerValidatorConsumer(BaseConsumer, ValidatorInterfaceMixin):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        if settings.DEBUG_TURN_AUTHENTICATION_OFF or settings.IS_LOCAL_MINER:
            self.my_hotkey = DONT_CHECK
        else:
            self.my_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
        self.validator_key = ""
        self.validator: Validator | None = None
        self.validator_authenticated = False
        self.msg_queue = []
        self.defer_saving_jobs = []
        self.defer_executor_ready = []
        self.pending_jobs: dict[str, AcceptedJob] = {}

    @log_errors_explicitly
    async def connect(self):
        await super().connect()
        # TODO verify ssl cert
        self.validator_key = self.scope["url_route"]["kwargs"]["validator_key"]
        fail = False
        msg = None
        try:
            self.validator = await Validator.objects.aget(public_key=self.validator_key)
        except Validator.DoesNotExist:
            msg = f"Unknown validator: {self.validator_key}"
            fail = True
        if (
            not settings.DEBUG_TURN_AUTHENTICATION_OFF
            and self.validator
            and not self.validator.active
        ):
            msg = f"Inactive validator: {self.validator_key}"
            fail = True
        if fail:
            await self.send(miner_requests.GenericError(details=msg).model_dump_json())
            logger.info(msg)
            await self.close(1000)
            return

        self.pending_jobs = await AcceptedJob.get_for_validator(self.validator)
        for job in self.pending_jobs.values():
            if job.status != AcceptedJob.Status.WAITING_FOR_PAYLOAD:
                # TODO: this actually works only for temporary connection issue between validator and miner;
                #       long running jobs would need to update job in regular periods, and actually would
                #       require complete refactor of communication scheme, so miner and validator can restart
                #       and still rebuild the connection and handle finished job execution... but right now
                #       losing connection between miner and executor is not recoverable, also restart of
                #       either validator or miner is unrecoverable, so when reading this take into account that
                #       this only handles this one particular case of broken connection between miner and validator
                if timezone.now() - job.updated_at > dt.timedelta(days=1):
                    job.status = AcceptedJob.Status.FAILED
                    # we don't want to block accepting connection - we defer it until after authorized
                    self.defer_saving_jobs.append(job)
                    logger.debug(f"Give up on job {job.job_uuid} after no status change for a day.")
                else:
                    await self.group_add(job.executor_token)
                continue
            if timezone.now() - job.updated_at > dt.timedelta(minutes=10):
                job.status = AcceptedJob.Status.FAILED
                # we don't want to block accepting connection - we defer it until after authorized
                self.defer_saving_jobs.append(job)
                logger.debug(
                    f"Give up on job {job.job_uuid} after not receiving payload after 10 minutes"
                )
            else:
                await self.group_add(job.executor_token)
                # we don't send anything until we get authorization confirmation
                self.defer_executor_ready.append(job)

    def accepted_request_type(self):
        return BaseValidatorRequest

    def incoming_generic_error_class(self):
        return validator_requests.GenericError

    def outgoing_generic_error_class(self):
        return miner_requests.GenericError

    def verify_auth_msg(self, msg: validator_requests.V0AuthenticateRequest) -> tuple[bool, str]:
        if msg.payload.timestamp < time.time() - AUTH_MESSAGE_MAX_AGE:
            return False, "msg too old"
        if not settings.IS_LOCAL_MINER and msg.payload.miner_hotkey != self.my_hotkey:
            return False, f"wrong miner hotkey ({self.my_hotkey}!={msg.payload.miner_hotkey})"
        if msg.payload.validator_hotkey != self.validator_key:
            return (
                False,
                f"wrong validator hotkey ({self.validator_key}!={msg.payload.validator_hotkey})",
            )

        keypair = bittensor.Keypair(ss58_address=self.validator_key)
        if keypair.verify(msg.blob_for_signing(), msg.signature):
            return True, ""

        return False, "Signature mismatches"

    def verify_receipt_msg(
        self,
        msg: validator_requests.V0JobStartedReceiptRequest
        | validator_requests.V0JobFinishedReceiptRequest,
    ) -> bool:
        if settings.IS_LOCAL_MINER:
            return True

        if self.my_hotkey != DONT_CHECK and msg.payload.miner_hotkey != self.my_hotkey:
            logger.warning(
                f"Miner hotkey mismatch in receipt for job_uuid {msg.payload.job_uuid} ({msg.payload.miner_hotkey!r} != {self.my_hotkey!r})"
            )
            return False
        if msg.payload.validator_hotkey != self.validator_key:
            logger.warning(
                f"Validator hotkey mismatch in receipt for job_uuid {msg.payload.job_uuid} ({msg.payload.validator_hotkey!r} != {self.validator_key!r})"
            )
            return False

        keypair = bittensor.Keypair(ss58_address=self.validator_key)
        if keypair.verify(msg.blob_for_signing(), msg.signature):
            return True

        logger.warning(
            f"Validator signature mismatch in receipt for job_uuid {msg.payload.job_uuid}"
        )
        return False

    async def handle_authentication(self, msg: validator_requests.V0AuthenticateRequest):
        if settings.DEBUG_TURN_AUTHENTICATION_OFF:
            logger.critical(
                f"Validator {self.validator_key} passed authentication without checking, because "
                f'"DEBUG_TURN_AUTHENTICATION_OFF" is on'
            )
        else:
            authenticated, error_msg = self.verify_auth_msg(msg)
            if not authenticated:
                response_msg = (
                    f"Validator {self.validator_key} not authenticated due to: {error_msg}"
                )
                logger.info(response_msg)
                await self.send(miner_requests.GenericError(details=response_msg).model_dump_json())
                await self.close(1000)
                return
        self.validator_authenticated = True
        manifest = await current.executor_manager.get_manifest()
        await self.send(
            miner_requests.V0ExecutorManifestRequest(
                manifest=miner_requests.ExecutorManifest(
                    executor_classes=[
                        miner_requests.ExecutorClassManifest(
                            executor_class=executor_class, count=count
                        )
                        for executor_class, count in manifest.items()
                    ]
                )
            ).model_dump_json()
        )
        for msg in self.msg_queue:
            await self.handle(msg)

        # we should not send any messages until validator authorizes itself
        for job in await AcceptedJob.get_not_reported(self.validator):
            if job.status == AcceptedJob.Status.FINISHED:
                await self.send(
                    miner_requests.V0JobFinishedRequest(
                        job_uuid=str(job.job_uuid),
                        docker_process_stdout=job.stdout,
                        docker_process_stderr=job.stderr,
                    ).model_dump_json()
                )
                logger.debug(
                    f"Job {job.job_uuid} finished reported to validator {self.validator_key}"
                )
            else:  # job.status == AcceptedJob.Status.FAILED:
                await self.send(
                    miner_requests.V0JobFailedRequest(
                        job_uuid=str(job.job_uuid),
                        docker_process_stdout=job.stdout,
                        docker_process_stderr=job.stderr,
                        docker_process_exit_status=job.exit_status,
                    ).model_dump_json()
                )
                logger.debug(
                    f"Failed job {job.job_uuid} reported to validator {self.validator_key}"
                )
            job.result_reported_to_validator = timezone.now()
            await job.asave()

        # we should not send any messages until validator authorizes itself
        while self.defer_executor_ready:
            job = self.defer_executor_ready.pop()
            await self.send(
                miner_requests.V0ExecutorReadyRequest(job_uuid=str(job.job_uuid)).model_dump_json()
            )
            logger.debug(
                f"Readiness for job {job.job_uuid} reported to validator {self.validator_key}"
            )

        # we could do this anywhere, but this sounds like a good enough place
        while self.defer_saving_jobs:
            job = self.defer_saving_jobs.pop()
            await job.asave()

    async def handle(self, msg: BaseValidatorRequest):
        if isinstance(msg, validator_requests.V0AuthenticateRequest):
            return await self.handle_authentication(msg)
        if not self.validator_authenticated:
            self.msg_queue.append(msg)
            return
        if isinstance(msg, validator_requests.V0InitialJobRequest):
            validator_blacklisted = await ValidatorBlacklist.objects.filter(
                validator=self.validator
            ).aexists()
            if validator_blacklisted:
                logger.info(
                    f"Declining job {msg.job_uuid} from blacklisted validator: {self.validator_key}"
                )
                await self.send(
                    miner_requests.V0DeclineJobRequest(job_uuid=msg.job_uuid).model_dump_json()
                )
                return
            # TODO add rate limiting per validator key here
            token = f"{msg.job_uuid}-{uuid.uuid4()}"
            await self.group_add(token)
            # let's create the job object before spinning up the executor, so if this process dies before getting
            # confirmation from the executor_manager the object is there and the executor will get the job details
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
                await current.executor_manager.reserve_executor(token)
            except ExecutorUnavailable:
                await self.send(
                    miner_requests.V0DeclineJobRequest(job_uuid=msg.job_uuid).model_dump_json()
                )
                await self.group_discard(token)
                await job.adelete()
                self.pending_jobs.pop(msg.job_uuid)
                return
            await self.send(
                miner_requests.V0AcceptJobRequest(job_uuid=msg.job_uuid).model_dump_json()
            )

        if isinstance(msg, validator_requests.V0JobRequest):
            job = self.pending_jobs.get(msg.job_uuid)
            if msg.volume and not msg.volume.is_safe():
                error_msg = f"Received JobRequest with unsafe volume: {msg.volume.contents}"
                logger.error(error_msg)
                await self.send(
                    miner_requests.GenericError(
                        details=error_msg,
                    ).model_dump_json()
                )
                return
            if job is None:
                error_msg = f"Received JobRequest for unknown job_uuid: {msg.job_uuid}"
                logger.error(error_msg)
                await self.send(
                    miner_requests.GenericError(
                        details=error_msg,
                    ).model_dump_json()
                )
                return
            await self.send_job_request(job.executor_token, msg)
            logger.debug(f"Passing job details to executor consumer job_uuid: {msg.job_uuid}")
            job.status = AcceptedJob.Status.RUNNING
            job.full_job_details = msg.model_dump()
            await job.asave()

        if isinstance(
            msg, validator_requests.V0JobStartedReceiptRequest
        ) and self.verify_receipt_msg(msg):
            logger.info(
                f"Received job started receipt for"
                f" job_uuid={msg.payload.job_uuid} validator_hotkey={msg.payload.validator_hotkey}"
                f" max_timeout={msg.payload.max_timeout}"
            )

            if settings.IS_LOCAL_MINER:
                return

            await JobStartedReceipt.objects.acreate(
                validator_signature=msg.signature,
                miner_signature=get_miner_signature(msg),
                job_uuid=msg.payload.job_uuid,
                miner_hotkey=msg.payload.miner_hotkey,
                validator_hotkey=msg.payload.validator_hotkey,
                executor_class=msg.payload.executor_class,
                time_accepted=msg.payload.time_accepted,
                max_timeout=msg.payload.max_timeout,
            )

        if isinstance(
            msg, validator_requests.V0JobFinishedReceiptRequest
        ) and self.verify_receipt_msg(msg):
            logger.info(
                f"Received job finished receipt for"
                f" job_uuid={msg.payload.job_uuid} validator_hotkey={msg.payload.validator_hotkey}"
                f" time_took={msg.payload.time_took} score={msg.payload.score}"
            )
            job = await AcceptedJob.objects.aget(job_uuid=msg.payload.job_uuid)
            job.time_took = msg.payload.time_took
            job.score = msg.payload.score
            await job.asave()

            if settings.IS_LOCAL_MINER:
                return

            await JobFinishedReceipt.objects.acreate(
                validator_signature=msg.signature,
                miner_signature=get_miner_signature(msg),
                job_uuid=msg.payload.job_uuid,
                miner_hotkey=msg.payload.miner_hotkey,
                validator_hotkey=msg.payload.validator_hotkey,
                time_started=msg.payload.time_started,
                time_took_us=msg.payload.time_took_us,
                score_str=msg.payload.score_str,
            )
            prepare_receipts.delay()

    async def _executor_ready(self, msg: ExecutorReady):
        job = await AcceptedJob.objects.aget(executor_token=msg.executor_token)
        self.pending_jobs[job.job_uuid] = job
        await self.send(
            miner_requests.V0ExecutorReadyRequest(job_uuid=str(job.job_uuid)).model_dump_json()
        )
        logger.debug(f"Readiness for job {job.job_uuid} reported to validator {self.validator_key}")

    async def _executor_failed_to_prepare(self, msg: ExecutorFailedToPrepare):
        jobs = [
            job for job in self.pending_jobs.values() if job.executor_token == msg.executor_token
        ]
        if not jobs:
            return
        job = jobs[0]
        self.pending_jobs = {
            k: v for k, v in self.pending_jobs.items() if v.executor_token != msg.executor_token
        }
        await self.send(
            miner_requests.V0ExecutorFailedRequest(job_uuid=job.job_uuid).model_dump_json()
        )
        logger.debug(
            f"Failure in preparation for job {job.job_uuid} reported to validator {self.validator_key}"
        )

    async def _executor_finished(self, msg: ExecutorFinished):
        await self.send(
            miner_requests.V0JobFinishedRequest(
                job_uuid=msg.job_uuid,
                docker_process_stdout=msg.docker_process_stdout,
                docker_process_stderr=msg.docker_process_stderr,
            ).model_dump_json()
        )
        logger.debug(f"Finished job {msg.job_uuid} reported to validator {self.validator_key}")
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = timezone.now()
        await job.asave()

    async def _executor_specs(self, msg: validator_requests.V0MachineSpecsRequest):
        await self.send(
            miner_requests.V0MachineSpecsRequest(
                job_uuid=msg.job_uuid,
                specs=msg.specs,
            ).model_dump_json()
        )
        logger.debug(
            f"Reported specs for job {msg.job_uuid}: {msg.specs} to validator {self.validator_key}"
        )

    async def _executor_failed(self, msg: ExecutorFailed):
        await self.send(
            miner_requests.V0JobFailedRequest(
                job_uuid=msg.job_uuid,
                docker_process_stdout=msg.docker_process_stdout,
                docker_process_stderr=msg.docker_process_stderr,
                docker_process_exit_status=msg.docker_process_exit_status,
            ).model_dump_json()
        )
        logger.debug(f"Failed job {msg.job_uuid} reported to validator {self.validator_key}")
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = timezone.now()
        await job.asave()

    async def disconnect(self, close_code):
        logger.info(f"Validator {self.validator_key} disconnected")
