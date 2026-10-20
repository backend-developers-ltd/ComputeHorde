import asyncio
import datetime as dt
import logging
import uuid
from datetime import timedelta
from functools import cached_property
from typing import Protocol, assert_never

from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde.protocol_consts import (
    TRUSTED_MINER_FAKE_KEY,
    HordeFailureReason,
    JobParticipantType,
    JobRejectionReason,
)
from compute_horde.protocol_messages import (
    GenericError,
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutionDoneRequest,
    V0ExecutorManifestRequest,
    V0ExecutorReadyRequest,
    V0HordeFailedRequest,
    V0InitialJobRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFailedRequest,
    V0JobFinishedReceiptRequest,
    V0JobFinishedRequest,
    V0JobRequest,
    V0MachineSpecsRequest,
    V0MainHotkeyMessage,
    V0StreamingJobReadyRequest,
    V0VolumesReadyRequest,
    ValidatorAuthForMiner,
    ValidatorToMinerMessage,
    parse_validator_to_miner_message,
)
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import ReceiptPayload
from compute_horde.utils import verify_blob_signature
from django.conf import settings
from django.db import IntegrityError
from django.db.models import DateTimeField, ExpressionWrapper, F
from django.utils import timezone

from compute_horde_miner.miner.executor_manager import current
from compute_horde_miner.miner.executor_manager.base import (
    AllExecutorsBusy,
    ExecutorUnavailable,
)
from compute_horde_miner.miner.miner_consumer.base_compute_horde_consumer import (
    BaseConsumer,
    log_errors_explicitly,
)
from compute_horde_miner.miner.miner_consumer.layer_utils import ValidatorInterfaceMixin
from compute_horde_miner.miner.models import (
    AcceptedJob,
    Validator,
    ValidatorBlacklist,
)
from compute_horde_miner.miner.receipts import current_store
from compute_horde_miner.miner.validator_auth import (
    AUTH_MESSAGE_MAX_CLOCK_SKEW,
    auth_signature_bytes,
    auth_timestamp_error,
    consume_auth_signature,
)

logger = logging.getLogger(__name__)

# Seconds a connected validator has to authenticate before the socket is closed.
AUTH_TIMEOUT = 5
# A validator dates the receipt right before sending the job request, but reconnecting to the
# miner can delay the request by up to about 5 minutes.
JOB_STARTED_RECEIPT_MAX_AGE = dt.timedelta(minutes=10)

DONT_CHECK = "DONT_CHECK"


class _RequestWithBlobForSigning(Protocol):
    def blob_for_signing(self) -> str: ...


def get_miner_signature(msg: _RequestWithBlobForSigning) -> str:
    keypair = settings.BITTENSOR_WALLET().get_hotkey()
    return f"0x{keypair.sign(msg.blob_for_signing()).hex()}"


class MinerValidatorConsumer(BaseConsumer[ValidatorToMinerMessage], ValidatorInterfaceMixin):
    class NotInitialized(Exception):
        pass

    class MissingExecutorToken(Exception):
        pass

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        if settings.DEBUG_TURN_AUTHENTICATION_OFF:
            self.my_hotkey = DONT_CHECK
        elif settings.IS_LOCAL_MINER:
            # Validators address their trusted miner by this key, not by its wallet's hotkey.
            self.my_hotkey = TRUSTED_MINER_FAKE_KEY
        else:
            self.my_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
        self.validator_authenticated = False
        self.msg_queue = []
        self.pending_jobs: dict[str, AcceptedJob] = {}
        self._auth_timeout_task: asyncio.Task[None] | None = None

        # Validator is populated only after the connection initialization succeeds
        self._maybe_validator: Validator | None = None

    @cached_property
    def validator(self) -> Validator:
        if not self._maybe_validator:
            raise MinerValidatorConsumer.NotInitialized("Missing validator")
        return self._maybe_validator

    @cached_property
    def validator_key(self) -> str:
        return str(self.scope["url_route"]["kwargs"]["validator_key"])

    @log_errors_explicitly
    async def connect(self):
        await super().connect()
        # TODO verify ssl cert

        try:
            self._maybe_validator = await Validator.objects.aget(public_key=self.validator_key)
        except Validator.DoesNotExist:
            await self.close_with_error_msg(f"Unknown validator: {self.validator_key}")
            return

        if not (self.validator.active or settings.DEBUG_TURN_AUTHENTICATION_OFF):
            await self.close_with_error_msg(f"Inactive validator: {self.validator_key}")
            return

        # The validator hotkey in the URL is public, so until the socket authenticates it must
        # not be subscribed to any job nor receive anything; see handle_authentication.
        self._auth_timeout_task = asyncio.create_task(self._close_on_auth_timeout())

    async def _close_on_auth_timeout(self):
        await asyncio.sleep(AUTH_TIMEOUT)
        await self.close_with_error_msg(
            f"Validator {self.validator_key} did not authenticate within {AUTH_TIMEOUT}s"
        )

    def _cancel_auth_timeout(self):
        if self._auth_timeout_task is not None:
            self._auth_timeout_task.cancel()
            self._auth_timeout_task = None

    async def _resume_pending_jobs(self) -> list[AcceptedJob]:
        """
        Re-subscribes to the validator's in-progress jobs after a reconnect, failing the ones
        that went stale; returns the jobs whose executor is still waiting for the payload.
        """
        self.pending_jobs = await AcceptedJob.get_for_validator(self.validator)
        jobs_awaiting_payload = []
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
                    await job.asave()
                    logger.debug(f"Give up on job {job.job_uuid} after no status change for a day.")
                else:
                    await self.group_add(job.executor_token)
                continue
            if timezone.now() - job.updated_at > dt.timedelta(minutes=10):
                job.status = AcceptedJob.Status.FAILED
                await job.asave()
                logger.debug(
                    f"Give up on job {job.job_uuid} after not receiving payload after 10 minutes"
                )
            else:
                await self.group_add(job.executor_token)
                jobs_awaiting_payload.append(job)
        return jobs_awaiting_payload

    async def close_with_error_msg(self, msg: str):
        await self.send(GenericError(details=msg).model_dump_json())
        logger.info(msg)
        await self.close(1000)

    async def verify_auth_msg(self, msg: ValidatorAuthForMiner) -> tuple[bool, str]:
        if timestamp_error := auth_timestamp_error(msg):
            return False, timestamp_error
        if msg.miner_hotkey != self.my_hotkey:
            return False, f"wrong miner hotkey ({self.my_hotkey}!={msg.miner_hotkey})"
        if msg.validator_hotkey != self.validator_key:
            return (
                False,
                f"wrong validator hotkey ({self.validator_key}!={msg.validator_hotkey})",
            )
        if (signature := auth_signature_bytes(msg)) is None:
            return False, "malformed signature"
        if not verify_blob_signature(self.validator_key, msg.blob_for_signing(), msg.signature):
            return False, "Signature mismatches"
        if not await consume_auth_signature(msg, signature):
            return False, "auth message already used"
        return True, ""

    def verify_receipt_payload(self, payload: ReceiptPayload, signature: str) -> bool:
        if self.my_hotkey != DONT_CHECK and payload.miner_hotkey != self.my_hotkey:
            logger.warning(
                f"Miner hotkey mismatch in receipt for job_uuid {payload.job_uuid} ({payload.miner_hotkey!r} != {self.my_hotkey!r})"
            )
            return False
        if payload.validator_hotkey != self.validator_key:
            logger.warning(
                f"Validator hotkey mismatch in receipt for job_uuid {payload.job_uuid} ({payload.validator_hotkey!r} != {self.validator_key!r})"
            )
            return False

        if verify_blob_signature(self.validator_key, payload.blob_for_signing(), signature):
            return True

        logger.warning(f"Validator signature mismatch in receipt for job_uuid {payload.job_uuid}")
        return False

    def job_request_signature_error(self, msg: V0InitialJobRequest | V0JobRequest) -> str | None:
        """
        Why the request cannot be attributed to the authenticated validator, or None.
        A missing signature is tolerated only when REQUIRE_SIGNED_JOB_REQUESTS is off.
        """
        if msg.validator_signature is None:
            if settings.REQUIRE_SIGNED_JOB_REQUESTS:
                return "missing validator signature"
            logger.warning(
                f"Accepting unsigned {msg.message_type} for job {msg.job_uuid} from validator "
                f"{self.validator_key} because REQUIRE_SIGNED_JOB_REQUESTS is off"
            )
            return None
        if not msg.is_signed_by(self.validator_key):
            return "invalid validator signature"
        return None

    async def decline_job(self, job_uuid: str, reason: JobRejectionReason, message: str) -> None:
        logger.info(f"Declining job {job_uuid} from validator {self.validator_key}: {message}")
        await self.send(
            V0DeclineJobRequest(job_uuid=job_uuid, reason=reason, message=message).model_dump_json()
        )

    async def handle_authentication(self, msg: ValidatorAuthForMiner):
        if self.validator_authenticated:
            logger.warning(
                f"Ignoring repeated authentication message from validator {self.validator_key}"
            )
            return

        if settings.DEBUG_TURN_AUTHENTICATION_OFF:
            logger.critical(
                f"Validator {self.validator_key} passed authentication without checking, because "
                f'"DEBUG_TURN_AUTHENTICATION_OFF" is on'
            )
        else:
            authenticated, auth_error_msg = await self.verify_auth_msg(msg)
            if not authenticated:
                close_error_msg = (
                    f"Validator {self.validator_key} not authenticated due to: {auth_error_msg}"
                )
                await self.close_with_error_msg(close_error_msg)
                return

        self.validator_authenticated = True
        self._cancel_auth_timeout()

        # (!) We should not send any messages until the validator authorizes itself.

        jobs_awaiting_payload = await self._resume_pending_jobs()

        manifest = await current.executor_manager.get_manifest()
        await self.send(V0ExecutorManifestRequest(manifest=manifest).model_dump_json())

        # Handle messages that may have arrived during the authentication
        for msg in self.msg_queue:
            await self.handle(msg)

        # TODO(post error propagation): decide if this is needed at all
        # disabled for now because this would need a migration and is likely not worth it
        # for job in await AcceptedJob.get_not_reported(self.validator):
        #     if job.status == AcceptedJob.Status.FINISHED:
        #         await self.send(
        #             V0JobFinishedRequest(
        #                 job_uuid=str(job.job_uuid),
        #                 docker_process_stdout=job.stdout,
        #                 docker_process_stderr=job.stderr,
        #                 artifacts=job.artifacts,
        #                 upload_results=job.upload_results,
        #             ).model_dump_json()
        #         )
        #         logger.debug(
        #             f"Job {job.job_uuid} finished reported to validator {self.validator_key}"
        #         )
        #     else:  # job.status == AcceptedJob.Status.FAILED:
        #         await self.send(
        #             V0JobFailedRequest(
        #                 job_uuid=str(job.job_uuid),
        #                 docker_process_stdout=job.stdout,
        #                 docker_process_stderr=job.stderr,
        #                 docker_process_exit_status=job.exit_status,
        #                 error_type=JobFailureReason(job.error_type)
        #                 if job.error_type
        #                 else None,
        #                 error_detail=job.error_detail,
        #             ).model_dump_json()
        #         )
        #         logger.debug(
        #             f"Failed job {job.job_uuid} reported to validator {self.validator_key}"
        #         )
        #     job.result_reported_to_validator = timezone.now()
        #     await job.asave()

        for job in jobs_awaiting_payload:
            await self.send(V0ExecutorReadyRequest(job_uuid=str(job.job_uuid)).model_dump_json())
            logger.debug(
                f"Readiness for job {job.job_uuid} reported to validator {self.validator_key}"
            )

    def parse_message(self, raw_msg: str | bytes) -> ValidatorToMinerMessage:
        return parse_validator_to_miner_message(raw_msg)

    async def handle(self, msg: ValidatorToMinerMessage) -> None:
        if self._maybe_validator is None:
            # An unknown validator should have received an error response from connect() by now.
            # All further incoming messages can be ignored.
            logger.warning(
                f"Dropping message {msg.__class__.__name__} from unknown validator {self.validator_key}"
            )
            return

        logger.debug(f"Received from validator {self.validator_key}: {msg.__class__.__name__}")

        if isinstance(msg, ValidatorAuthForMiner):
            await self.handle_authentication(msg)
            return

        if not self.validator_authenticated:
            self.msg_queue.append(msg)
            return

        if isinstance(msg, V0InitialJobRequest | V0JobRequest):
            if signature_error := self.job_request_signature_error(msg):
                await self.decline_job(
                    msg.job_uuid, JobRejectionReason.INVALID_SIGNATURE, signature_error
                )
                return
            # Proactively check volume safety in both requests that may contain a volume
            if msg.volume and not msg.volume.is_safe():
                error_msg = f"Received JobRequest with unsafe volume: {msg.volume}"
                logger.error(error_msg)
                await self.send(GenericError(details=error_msg).model_dump_json())
                return

        if isinstance(msg, V0InitialJobRequest):
            await self.handle_initial_job_request(msg)
            return

        if isinstance(msg, V0JobRequest):
            await self.handle_job_request(msg)
            return

        if isinstance(msg, V0JobAcceptedReceiptRequest):
            await self.handle_job_accepted_receipt(msg)
            return

        if isinstance(msg, V0JobFinishedReceiptRequest):
            await self.handle_job_finished_receipt(msg)
            return

        if isinstance(msg, GenericError):
            logger.error(f"Received GenericError from validator: {msg.details}")
            # Nothing to do here - this doesn't tell us whether the job is dead.
            return

        if isinstance(msg, V0MainHotkeyMessage):
            await self.handle_main_hotkey_request(msg)
            return

        assert_never(msg)

    async def handle_initial_job_request(self, msg: V0InitialJobRequest):
        validator_blacklisted = await ValidatorBlacklist.objects.filter(
            validator=self.validator
        ).aexists()
        if validator_blacklisted:
            await self.decline_job(
                msg.job_uuid, JobRejectionReason.VALIDATOR_BLACKLISTED, "validator is blacklisted"
            )
            return

        if receipt_error := await self.accept_job_started_receipt(msg):
            await self.decline_job(msg.job_uuid, JobRejectionReason.INVALID_RECEIPT, receipt_error)
            return

        # TODO add rate limiting per validator key here
        executor_token = f"{msg.job_uuid}-{uuid.uuid4()}"
        await self.group_add(executor_token)
        # let's create the job object before spinning up the executor, so if this process dies before getting
        # confirmation from the executor_manager the object is there and the executor will get the job details
        job = AcceptedJob(
            validator=self.validator,
            job_uuid=msg.job_uuid,
            executor_token=executor_token,
            initial_job_details=msg.model_dump(),
            status=AcceptedJob.Status.WAITING_FOR_EXECUTOR,
        )
        await job.asave()
        self.pending_jobs[msg.job_uuid] = job

        # This will reserve AND start the executor.
        # Note: the timeout is only passed to the vendor executor manager for reference.
        # This will NOT time out by itself.
        executor_spinup = asyncio.create_task(
            current.executor_manager.reserve_executor_class(
                executor_token,
                msg.executor_class,
                timeout=EXECUTOR_CLASS[msg.executor_class].spin_up_time,
            ),
        )

        try:
            # Wait for a signal from the executor_manager that the executor is reserved.
            # This should be a fast operation, so the validator only waits for a short time for this.
            await current.executor_manager.wait_for_executor_reservation(
                executor_token,
                msg.executor_class,
            )

            # If there is no executor, the above future throws an appropriate exception,
            # so we will never proceed further down.
            await self.send(V0AcceptJobRequest(job_uuid=msg.job_uuid).model_dump_json())

            # Then, wait for the executor to actually start up.
            executor = await executor_spinup
            executor_address = await current.executor_manager.get_executor_public_address(executor)
            job.executor_address = executor_address
            await job.asave()

        except AllExecutorsBusy:
            await self.group_discard(executor_token)
            job.status = AcceptedJob.Status.REJECTED
            await job.asave()
            self.pending_jobs.pop(msg.job_uuid)
            now = msg.job_started_receipt_payload.timestamp
            queryset = (
                JobStartedReceipt.objects.annotate(
                    valid_until=ExpressionWrapper(
                        F("timestamp") + F("ttl") * timedelta(seconds=1),
                        output_field=DateTimeField(),
                    ),
                )
                .filter(
                    is_organic=True,
                    executor_class=msg.executor_class,
                    timestamp__lte=now,
                    valid_until__gte=now,
                    miner_signature__isnull=False,  # miner signature is needed to build a valid Receipt
                )
                .exclude(
                    job_uuid=msg.job_uuid,  # UUIDField doesn't support "__ne=..."
                )
            )
            receipts = [receipt async for receipt in queryset]
            logger.info(
                f"Declining job {msg.job_uuid}: all executors busy. Sending {len(receipts)} excuse receipts:"
            )
            for receipt in receipts:
                valid_until = receipt.timestamp + timedelta(seconds=receipt.ttl)
                logger.debug(
                    f"Receipt for job {receipt.job_uuid} from validator {receipt.validator_hotkey} valid until {valid_until}"
                )
            await self.send(
                V0DeclineJobRequest(
                    job_uuid=msg.job_uuid,
                    reason=JobRejectionReason.BUSY,
                    receipts=[r.to_receipt() for r in receipts],
                ).model_dump_json()
            )

        except ExecutorUnavailable:
            await self.group_discard(executor_token)
            job.status = AcceptedJob.Status.FAILED
            await job.asave()
            self.pending_jobs.pop(msg.job_uuid)
            logger.info(f"Failing job {msg.job_uuid}: executor failed to start")
            await self.send(
                V0HordeFailedRequest(
                    job_uuid=msg.job_uuid,
                    reported_by=JobParticipantType.MINER,
                    reason=HordeFailureReason.EXECUTOR_SPINUP_FAILED,
                    message="Executor failed to start",
                ).model_dump_json()
            )

        finally:
            # In any case, the spinup task must be cleaned up.
            executor_spinup.cancel()
            try:
                await executor_spinup
            except (asyncio.CancelledError, Exception):
                # As we're awaiting this task for the second time, just silence the exceptions as
                # they have been already thrown during the previous await.
                pass

    async def handle_job_request(self, msg: V0JobRequest):
        job = self.pending_jobs.get(msg.job_uuid)
        if job is None:
            error_msg = f"Received JobRequest for unknown job_uuid: {msg.job_uuid}"
            logger.error(error_msg)
            await self.send(GenericError(details=error_msg).model_dump_json())
            return

        if job.initial_job_details.get("volume") is not None and msg.volume is not None:
            # The volume may have been already sent in the initial job request.
            error_msg = f"Received job volume twice job_uuid: {msg.job_uuid}"
            logger.error(error_msg)
            await self.send(GenericError(details=error_msg).model_dump_json())
            return

        await self.send_job_request(job.executor_token, msg)
        logger.debug(f"Passing job details to executor consumer job_uuid: {msg.job_uuid}")
        job.status = AcceptedJob.Status.RUNNING
        job.full_job_details = msg.model_dump()
        await job.asave()

    async def accept_job_started_receipt(self, msg: V0InitialJobRequest) -> str | None:
        """
        Verifies the job-started receipt carried by the request and records it, unless this is a
        local miner, which keeps no receipts; returns why the receipt was refused, or None.
        Each receipt is accepted once and only while it is recent, so that a captured request
        cannot start its job again.
        """
        payload = msg.job_started_receipt_payload
        signature = msg.job_started_receipt_signature
        logger.info(
            f"Received job started receipt for"
            f" job_uuid={payload.job_uuid} validator_hotkey={payload.validator_hotkey}"
        )

        if payload.job_uuid != msg.job_uuid:
            return f"job started receipt is for job {payload.job_uuid}"
        if payload.executor_class != msg.executor_class:
            return f"job started receipt is for executor class {payload.executor_class}"
        if not self.verify_receipt_payload(payload, signature):
            return "job started receipt could not be verified"
        now = timezone.now()
        if payload.timestamp < now - JOB_STARTED_RECEIPT_MAX_AGE:
            return "job started receipt is too old"
        if payload.timestamp > now + dt.timedelta(seconds=AUTH_MESSAGE_MAX_CLOCK_SKEW):
            return "job started receipt is dated in the future"
        if settings.IS_LOCAL_MINER:
            if await AcceptedJob.objects.filter(
                validator=self.validator, job_uuid=msg.job_uuid
            ).aexists():
                return "job was already accepted"
            return None

        try:
            receipt = await JobStartedReceipt.objects.acreate(
                job_uuid=payload.job_uuid,
                validator_hotkey=payload.validator_hotkey,
                miner_hotkey=payload.miner_hotkey,
                validator_signature=signature,
                miner_signature=get_miner_signature(payload),
                timestamp=payload.timestamp,
                executor_class=payload.executor_class,
                is_organic=payload.is_organic,
                ttl=payload.ttl,
            )
        except IntegrityError:
            return "job started receipt was already used"
        (await current_store()).store([receipt.to_receipt()])
        return None

    async def handle_job_accepted_receipt(self, msg: V0JobAcceptedReceiptRequest):
        logger.info(
            f"Received job accepted receipt for"
            f" job_uuid={msg.payload.job_uuid} validator_hotkey={msg.payload.validator_hotkey}"
        )

        if not self.verify_receipt_payload(msg.payload, msg.signature):
            logger.error("Receipt could not be verified")
            return

        if settings.IS_LOCAL_MINER:
            return

        created_receipt = await JobAcceptedReceipt.objects.acreate(
            validator_signature=msg.signature,
            miner_signature=get_miner_signature(msg),
            job_uuid=msg.payload.job_uuid,
            miner_hotkey=msg.payload.miner_hotkey,
            validator_hotkey=msg.payload.validator_hotkey,
            timestamp=msg.payload.timestamp,
            time_accepted=msg.payload.time_accepted,
            ttl=msg.payload.ttl,
        )

        (await current_store()).store([created_receipt.to_receipt()])

    async def handle_job_finished_receipt(self, msg: V0JobFinishedReceiptRequest):
        logger.info(
            f"Received job finished receipt for"
            f" job_uuid={msg.payload.job_uuid} validator_hotkey={msg.payload.validator_hotkey}"
            f" time_took={msg.payload.time_took} score={msg.payload.score}"
        )

        if not self.verify_receipt_payload(msg.payload, msg.signature):
            logger.error("Receipt could not be verified")
            return

        job = await AcceptedJob.objects.aget(job_uuid=msg.payload.job_uuid)
        job.time_took = msg.payload.time_took
        job.score = msg.payload.score
        await job.asave()

        if settings.IS_LOCAL_MINER:
            return

        created_receipt = await JobFinishedReceipt.objects.acreate(
            validator_signature=msg.signature,
            miner_signature=get_miner_signature(msg),
            job_uuid=msg.payload.job_uuid,
            miner_hotkey=msg.payload.miner_hotkey,
            validator_hotkey=msg.payload.validator_hotkey,
            timestamp=msg.payload.timestamp,
            time_started=msg.payload.time_started,
            time_took_us=msg.payload.time_took_us,
            score_str=msg.payload.score_str,
            block_numbers=msg.payload.block_numbers,
        )

        (await current_store()).store([created_receipt.to_receipt()])

    async def handle_main_hotkey_request(self, msg: V0MainHotkeyMessage):
        logger.info(f"Received main hotkey request from validator {self.validator_key}")

        if not self.validator_authenticated:
            logger.warning(
                f"Received main hotkey request from unauthenticated validator {self.validator_key}"
            )
            await self.send(GenericError(details="Unauthenticated validator").model_dump_json())
            return

        # Get main hotkey from executor manager
        main_hotkey = await current.executor_manager.get_main_hotkey()

        await self.send(V0MainHotkeyMessage(main_hotkey=main_hotkey).model_dump_json())
        logger.info(f"Sent main hotkey to validator {self.validator_key}: {main_hotkey}")

    async def _executor_ready(self, msg: V0ExecutorReadyRequest):
        logger.debug(f"_executor_ready for {msg}")
        if self.check_missing_token(msg):
            return
        job = await AcceptedJob.objects.aget(executor_token=msg.executor_token)
        job_uuid = str(job.job_uuid)
        assert job_uuid == msg.job_uuid
        self.pending_jobs[job_uuid] = job
        await self.send(msg.model_copy(update={"executor_token": None}).model_dump_json())
        logger.debug(f"Readiness for job {job_uuid} reported to validator {self.validator_key}")

    async def _streaming_job_ready(self, msg: V0StreamingJobReadyRequest):
        if self.check_missing_token(msg):
            return
        job = await AcceptedJob.objects.aget(executor_token=msg.executor_token)
        job_uuid = str(job.job_uuid)
        assert job_uuid == msg.job_uuid
        new_msg = msg.model_copy(
            update={
                "executor_token": None,
                "miner_signature": get_miner_signature(msg),
            }
        )
        await self.send(new_msg.model_dump_json())
        logger.debug(
            f"Streaming readiness for job {job_uuid} reported to validator {self.validator_key}"
        )

    def check_missing_token(self, msg: V0ExecutorReadyRequest | V0StreamingJobReadyRequest):
        if msg.executor_token is None:
            raise MinerValidatorConsumer.MissingExecutorToken(
                f"Validator Client received {msg} with missing executor_token"
            )

    async def _executor_finished(self, msg: V0JobFinishedRequest):
        await self.send(msg.model_dump_json())
        logger.debug(f"Finished job {msg.job_uuid} reported to validator {self.validator_key}")
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = timezone.now()
        await job.asave()

    async def _executor_specs(self, msg: V0MachineSpecsRequest):
        await self.send(msg.model_dump_json())
        logger.debug(
            f"Reported specs for job {msg.job_uuid}: {msg.specs} to validator {self.validator_key}"
        )

    async def _job_rejected(self, msg: V0DeclineJobRequest):
        await self.send(msg.model_dump_json())
        logger.debug(f"Rejected job {msg.job_uuid} reported to validator {self.validator_key}")
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = timezone.now()
        await job.asave()

    async def _job_failed(self, msg: V0JobFailedRequest):
        await self.send(msg.model_dump_json())
        logger.debug(f"Failed job {msg.job_uuid} reported to validator {self.validator_key}")
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = timezone.now()
        await job.asave()

    async def _horde_failed(self, msg: V0HordeFailedRequest):
        await self.send(msg.model_dump_json())
        logger.debug(
            f"Horde failed at job {msg.job_uuid} reported to validator {self.validator_key}"
        )
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = timezone.now()
        await job.asave()

    async def _volumes_ready(self, msg: V0VolumesReadyRequest):
        await self.send(msg.model_dump_json())

    async def _execution_done(self, msg: V0ExecutionDoneRequest):
        await self.send(msg.model_dump_json())

    async def disconnect(self, close_code):
        self._cancel_auth_timeout()
        logger.info(f"Validator {self.validator_key} disconnected")
