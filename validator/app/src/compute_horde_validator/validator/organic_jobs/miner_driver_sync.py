import datetime
import logging
import time
from collections.abc import Callable
from enum import Enum, auto
from typing import TypeVar, assert_never

import bittensor
import bittensor_wallet
import sentry_sdk
import websockets.sync.client
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest
from compute_horde.fv_protocol.validator_requests import (
    HordeFailureDetails,
    JobFailureDetails,
    JobRejectionDetails,
    JobResultDetails,
    JobStatusMetadata,
    JobStatusUpdate,
    StreamingServerDetails,
)
from compute_horde.job_errors import HordeError
from compute_horde.protocol_consts import (
    HordeFailureReason,
    JobFailureReason,
    JobParticipantType,
    JobRejectionReason,
    JobStatus,
)
from compute_horde.protocol_messages import (
    FailureContext,
    GenericError,
    MinerToValidatorMessage,
    UnauthorizedError,
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutionDoneRequest,
    V0ExecutorFailedRequest,
    V0ExecutorManifestRequest,
    V0ExecutorReadyRequest,
    V0HordeFailedRequest,
    V0InitialJobRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFailedRequest,
    V0JobFinishedReceiptRequest,
    V0JobFinishedRequest,
    V0JobRequest,
    V0StreamingJobNotReadyRequest,
    V0StreamingJobReadyRequest,
    V0VolumesReadyRequest,
    ValidatorAuthForMiner,
    ValidatorToMinerMessage,
)
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.utils import Timer, sign_blob
from django.conf import settings
from pydantic import TypeAdapter

from compute_horde_validator.validator import job_excuses
from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.types import ValidatorModel
from compute_horde_validator.validator.allowance.utils.supertensor import supertensor
from compute_horde_validator.validator.dynamic_config import get_config
from compute_horde_validator.validator.models import (
    Miner,
    OrganicJob,
    SystemEvent,
)
from compute_horde_validator.validator.routing.default import routing
from compute_horde_validator.validator.routing.types import JobRoute, MinerIncidentType
from compute_horde_validator.validator.utils import TRUSTED_MINER_FAKE_KEY

logger = logging.getLogger(__name__)

_MinerMsgT = TypeVar("_MinerMsgT", bound=MinerToValidatorMessage)


def _get_current_block_sync() -> int:
    if settings.DEBUG_USE_MOCK_BLOCK_NUMBER:
        block = 5136476 + int((time.time() - 1742076533) / 12)
    else:
        block = allowance().get_current_block()
    return block


def _get_active_validators(block: int | None) -> list[ValidatorModel]:
    if settings.DEBUG_USE_MOCK_BLOCK_NUMBER:
        # TODO: this is here for testing. probably should not be merged.
        return []

    if block is None:
        block = supertensor().get_current_block()
    return supertensor().list_validators(block)


class MinerClient:
    # TODO: add retries

    def __init__(
        self,
        url: str,
        miner_hotkey: str,
        validator_keypair: bittensor.Keypair,
        connect_timeout: float = 10.0,
        recv_timeout: float = 5.0,
    ) -> None:
        self.url = url
        self.miner_hotkey = miner_hotkey
        self.validator_keypair = validator_keypair
        self.connect_timeout = connect_timeout
        self.recv_timeout = recv_timeout
        self.ws: websockets.sync.client.ClientConnection | None = None

    def connect(self) -> None:
        self.ws = websockets.sync.client.connect(
            self.url,
            open_timeout=self.connect_timeout,
            close_timeout=self.recv_timeout,
            max_size=50 * (2**20),  # 50MB
        )

        # TODO: After async organic jobs is removed, move auth to connection headers
        #       instead of separate message
        msg = ValidatorAuthForMiner(
            validator_hotkey=self.validator_keypair.ss58_address,
            miner_hotkey=self.miner_hotkey,
            timestamp=int(time.time()),
            signature="",
        )
        msg.signature = sign_blob(self.validator_keypair, msg.blob_for_signing())
        self.ws.send(msg.model_dump_json())

    def close(self) -> None:
        try:
            if self.ws:
                self.ws.close()
        finally:
            self.ws = None

    def send(self, msg: ValidatorToMinerMessage) -> None:
        assert self.ws is not None
        raw = msg.model_dump_json()
        logger.debug("Sending message to miner %s: %s", self.miner_hotkey, raw)
        self.ws.send(raw)

    def recv(self) -> MinerToValidatorMessage:
        assert self.ws is not None
        raw = self.ws.recv(timeout=self.recv_timeout, decode=False)
        logger.debug("Received message from miner %s: %s", self.miner_hotkey, raw)
        return TypeAdapter(MinerToValidatorMessage).validate_json(raw)


class DriverState(Enum):
    CONNECT = auto()
    RESERVE_EXECUTOR = auto()
    WAIT_JOB_ACCEPTED = auto()
    SEND_JOB_ACCEPTED_RECEIPT = auto()
    WAIT_EXECUTOR_READY = auto()
    PREPARE_VOLUMES = auto()
    WAIT_VOLUMES_READY = auto()
    WAIT_STREAMING_JOB_READY = auto()
    WAIT_EXECUTION_DONE = auto()
    COLLECT_RESULTS = auto()
    COMPLETE = auto()
    FAILED = auto()


_job_event_subtype_map: dict[JobFailureReason, SystemEvent.EventSubType] = {
    JobFailureReason.UNKNOWN: SystemEvent.EventSubType.GENERIC_ERROR,
    JobFailureReason.TIMEOUT: SystemEvent.EventSubType.JOB_TIMEOUT,
    JobFailureReason.NONZERO_RETURN_CODE: SystemEvent.EventSubType.JOB_PROCESS_NONZERO_EXIT_CODE,
    JobFailureReason.DOWNLOAD_FAILED: SystemEvent.EventSubType.JOB_VOLUME_DOWNLOAD_FAILED,
    JobFailureReason.UPLOAD_FAILED: SystemEvent.EventSubType.JOB_RESULT_UPLOAD_FAILED,
}

_horde_event_subtype_map: dict[HordeFailureReason, SystemEvent.EventSubType] = {
    HordeFailureReason.MINER_CONNECTION_FAILED: SystemEvent.EventSubType.MINER_CONNECTION_ERROR,
    HordeFailureReason.INITIAL_RESPONSE_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.EXECUTOR_READINESS_RESPONSE_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.STREAMING_JOB_READY_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.VOLUMES_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.EXECUTION_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.FINAL_RESPONSE_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.SECURITY_CHECK_FAILED: SystemEvent.EventSubType.ERROR_FAILED_SECURITY_CHECK,
    HordeFailureReason.STREAMING_FAILED: SystemEvent.EventSubType.GENERIC_ERROR,
    HordeFailureReason.UNHANDLED_EXCEPTION: SystemEvent.EventSubType.GENERIC_ERROR,
    HordeFailureReason.UNKNOWN: SystemEvent.EventSubType.GENERIC_ERROR,
}


class SyncOrganicJobDriver:
    def __init__(
        self,
        miner_client: MinerClient,
        job: OrganicJob,
        request: OrganicJobRequest,
        *,
        miner_hotkey: str,
        my_keypair: bittensor.Keypair,
        allowed_leeway: int,
        reservation_time_limit: int,
        executor_startup_time_limit: int,
        max_overall_time_limit: int,
        status_callback: Callable[[JobStatusUpdate], None] | None = None,
    ) -> None:
        self.miner_client = miner_client
        self.job = job
        self.request = request
        self.miner_hotkey = miner_hotkey
        self.my_keypair = my_keypair

        self.allowed_leeway = allowed_leeway
        self.reservation_time_limit = reservation_time_limit
        self.executor_startup_time_limit = executor_startup_time_limit

        self.status_callback = status_callback or (lambda _status: None)
        self._state = DriverState.CONNECT
        self._started_at = time.time()
        self._global_deadline = self._started_at + max_overall_time_limit
        self._deadline: Timer | None = None

    def _record_event(
        self,
        type: SystemEvent.EventType,
        subtype: SystemEvent.EventSubType,
        long_description: str,
    ) -> None:
        if self.job.on_trusted_miner and get_config("DYNAMIC_DISABLE_TRUSTED_ORGANIC_JOB_EVENTS"):
            return

        event_data = {"job_uuid": str(self.job.job_uuid), "miner_hotkey": self.miner_hotkey}

        SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
            type=type,
            subtype=subtype,
            long_description=long_description,
            data=event_data,
        )

    def _report_incident(self, incident_type: MinerIncidentType) -> None:
        routing().report_miner_incident(
            incident_type,
            hotkey_ss58address=self.job.miner.hotkey,
            job_uuid=str(self.job.job_uuid),
            executor_class=self.request.executor_class,
        )

    def _handle_horde_failure(
        self,
        comment: str,
        event_subtype: SystemEvent.EventSubType,
        horde_failure_reason: HordeFailureReason,
        reported_by: JobParticipantType = JobParticipantType.VALIDATOR,
        horde_failure_context: FailureContext | None = None,
        incident_type: MinerIncidentType | None = None,
        log_exc_info: bool = False,
    ) -> DriverState:
        logger.warning(comment, exc_info=log_exc_info)
        self.job.status = OrganicJob.Status.FAILED
        self.job.comment = comment
        self.job.save(update_fields=["status", "comment"])

        self._record_event(
            type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
            subtype=event_subtype,
            long_description=comment,
        )

        if incident_type:
            self._report_incident(incident_type)

        self.status_callback(
            JobStatusUpdate(
                uuid=str(self.job.job_uuid),
                status=JobStatus.FAILED,
                metadata=JobStatusMetadata(
                    horde_failure_details=HordeFailureDetails(
                        reported_by=reported_by,
                        reason=horde_failure_reason,
                        message=comment,
                        context=horde_failure_context,
                    ),
                ),
            )
        )
        return DriverState.FAILED

    def _handle_decline_job(self, msg: V0DeclineJobRequest) -> DriverState:
        """Handle job decline/rejection from miner, including excuse validation."""
        if msg.reason != JobRejectionReason.BUSY:
            comment = f"Miner rejected job: {msg.message}"
            self.job.status = OrganicJob.Status.FAILED
            system_event_subtype = SystemEvent.EventSubType.JOB_REJECTED
        else:
            job_started_receipt = JobStartedReceipt.objects.get(job_uuid=str(self.job.job_uuid))
            job_request_time = job_started_receipt.timestamp
            active_validators = _get_active_validators(self.job.block)
            valid_excuses = job_excuses.filter_valid_excuse_receipts(
                receipts_to_check=msg.receipts or [],
                check_time=job_request_time,
                declined_job_uuid=str(self.job.job_uuid),
                declined_job_executor_class=self.request.executor_class,
                declined_job_is_synthetic=False,
                minimum_validator_stake_for_excuse=get_config(
                    "DYNAMIC_MINIMUM_VALIDATOR_STAKE_FOR_EXCUSE"
                ),
                miner_hotkey=self.job.miner.hotkey,
                active_validators=active_validators,
            )
            expected_executor_count = job_excuses.get_expected_miner_executor_count_sync(
                check_time=job_request_time,
                miner_hotkey=self.job.miner.hotkey,
                executor_class=self.request.executor_class,
            )
            if len(valid_excuses) >= expected_executor_count:
                comment = "Miner properly excused job"
                self.job.status = OrganicJob.Status.EXCUSED
                system_event_subtype = SystemEvent.EventSubType.JOB_EXCUSED
            else:
                comment = "Miner failed to excuse job"
                self.job.status = OrganicJob.Status.FAILED
                system_event_subtype = SystemEvent.EventSubType.JOB_REJECTED

        logger.info(comment)
        self.job.comment = comment
        self.job.save(update_fields=["status", "comment"])

        if self.job.status != OrganicJob.Status.EXCUSED:
            self._report_incident(MinerIncidentType.MINER_JOB_REJECTED)

        self.status_callback(
            JobStatusUpdate(
                uuid=str(self.job.job_uuid),
                status=JobStatus.REJECTED,
                metadata=JobStatusMetadata(
                    job_rejection_details=JobRejectionDetails(
                        rejected_by=JobParticipantType.MINER,
                        reason=msg.reason,
                        message=comment,
                        context=msg.context,
                    ),
                ),
            )
        )

        self._record_event(
            type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
            subtype=system_event_subtype,
            long_description=comment,
        )
        return DriverState.FAILED

    def _handle_job_failed(self, msg: V0JobFailedRequest) -> DriverState:
        """Handle V0JobFailedRequest from miner."""
        comment = msg.message or "Miner reported job failure"
        logger.info("Miner reported job failure: %s", comment)
        self.job.status = OrganicJob.Status.FAILED
        self.job.comment = comment
        self.job.save(update_fields=["status", "comment"])

        self._report_incident(MinerIncidentType.MINER_JOB_FAILED)
        subtype = _job_event_subtype_map.get(
            msg.reason, SystemEvent.EventSubType.GENERIC_JOB_FAILURE
        )
        self.status_callback(
            JobStatusUpdate(
                uuid=str(self.job.job_uuid),
                status=JobStatus.FAILED,
                metadata=JobStatusMetadata(
                    job_failure_details=JobFailureDetails(
                        reason=msg.reason,
                        stage=msg.stage,
                        message=msg.message,
                        context=msg.context,
                        docker_process_exit_status=msg.docker_process_exit_status,
                        docker_process_stdout=msg.docker_process_stdout,
                        docker_process_stderr=msg.docker_process_stderr,
                    ),
                ),
            )
        )

        self._record_event(
            type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
            subtype=subtype,
            long_description=comment,
        )
        return DriverState.FAILED

    def _handle_horde_failed(self, msg: V0HordeFailedRequest) -> DriverState:
        """Handle V0HordeFailedRequest from miner."""
        comment = msg.message or "Miner reported horde failure"
        logger.info("Miner reported horde failure: %s", comment)
        self.job.status = OrganicJob.Status.FAILED
        self.job.comment = comment
        self.job.save(update_fields=["status", "comment"])

        self._report_incident(MinerIncidentType.MINER_HORDE_FAILED)
        subtype = _horde_event_subtype_map.get(msg.reason, SystemEvent.EventSubType.GENERIC_ERROR)
        self.status_callback(
            JobStatusUpdate(
                uuid=str(self.job.job_uuid),
                status=JobStatus.HORDE_FAILED,
                metadata=JobStatusMetadata(
                    horde_failure_details=HordeFailureDetails(
                        reported_by=msg.reported_by,
                        reason=msg.reason,
                        message=msg.message,
                        context=msg.context,
                    ),
                ),
            )
        )

        self._record_event(
            type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
            subtype=subtype,
            long_description=comment,
        )
        return DriverState.FAILED

    def _wait_for_message(
        self,
        expected_message_type: type[_MinerMsgT],
        timeout: float,
        timeout_status_reason: HordeFailureReason,
    ) -> _MinerMsgT | DriverState:
        """
        Waits for a specific message type from the miner within a given timeout period.
        Returns the received message if successful.
        Returns a terminal DriverState if the timeout is reached or if an error message is received.
        """
        deadline = time.time() + timeout
        while True:
            if time.time() >= min(deadline, self._global_deadline):
                # NOTE: No incident reported - timeouts are not miner incidents,
                # consistent with the async implementation.
                return self._handle_horde_failure(
                    comment=f"Timed out waiting for miner (reason={timeout_status_reason})",
                    event_subtype=SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
                    horde_failure_reason=timeout_status_reason,
                )

            try:
                msg = self.miner_client.recv()
            except TimeoutError:
                # Individual recv timeouts are not considered errors. Timeouts are handled above.
                continue

            if isinstance(msg, V0ExecutorManifestRequest):
                # NOTE: they are not used in organic jobs
                continue

            # Validate that the message is for the correct job
            received_job_uuid = getattr(msg, "job_uuid", str(self.job.job_uuid))
            if received_job_uuid != str(self.job.job_uuid):
                logger.warning(
                    "Received msg for a different job (expected job %s, got job %s): %s",
                    self.job.job_uuid,
                    received_job_uuid,
                    msg,
                )
                continue

            if isinstance(msg, expected_message_type):
                return msg

            # Handle legacy messages by mapping them to V0HordeFailedRequest
            if isinstance(msg, V0StreamingJobNotReadyRequest):
                msg = V0HordeFailedRequest(
                    job_uuid=msg.job_uuid,
                    reported_by=JobParticipantType.MINER,
                    reason=HordeFailureReason.STREAMING_FAILED,
                    message="Executor reported legacy V0StreamingJobNotReadyRequest message",
                )
            elif isinstance(msg, V0ExecutorFailedRequest):
                msg = V0HordeFailedRequest(
                    job_uuid=msg.job_uuid,
                    reported_by=JobParticipantType.MINER,
                    reason=HordeFailureReason.GENERIC_ERROR,
                    message="Executor reported legacy V0ExecutorFailedRequest message",
                )

            if isinstance(msg, GenericError):
                closing_phrases = ["Unknown validator", "Inactive validator", "not authenticated"]
                # TODO: move these messages from GenericError to UnauthorizedError in miner
                for close_phrase in closing_phrases:
                    if msg.details and close_phrase in msg.details:
                        return self._handle_horde_failure(
                            comment=f"Miner closed connection: {msg.details}",
                            event_subtype=SystemEvent.EventSubType.UNAUTHORIZED,
                            horde_failure_reason=HordeFailureReason.MINER_CONNECTION_FAILED,
                            reported_by=JobParticipantType.MINER,
                            incident_type=MinerIncidentType.MINER_JOB_REJECTED,
                        )
                else:
                    self._record_event(
                        type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
                        subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                        long_description=f"Received error message from miner {self.miner_hotkey}: {msg.model_dump_json()}",
                    )
                    # NOTE: To align with the old behavior, we do not mark the job as failed here.
                    # In theory, miner can still continue after sending a GenericError.
                    continue

            if isinstance(msg, UnauthorizedError):
                return self._handle_horde_failure(
                    comment=f"Miner returned unauthorized error: {msg.code} {msg.details}",
                    event_subtype=SystemEvent.EventSubType.UNAUTHORIZED,
                    horde_failure_reason=HordeFailureReason.MINER_CONNECTION_FAILED,
                    reported_by=JobParticipantType.MINER,
                )

            if isinstance(msg, V0DeclineJobRequest):
                return self._handle_decline_job(msg)

            if isinstance(msg, V0JobFailedRequest):
                return self._handle_job_failed(msg)

            if isinstance(msg, V0HordeFailedRequest):
                return self._handle_horde_failed(msg)

            logger.info(
                "Ignoring %s while waiting for %s for job %s",
                msg.__class__.__name__,
                expected_message_type.__name__,
                self.job.job_uuid,
            )

    def run(self) -> None:
        try:
            self._run()
        except Exception as exc:
            sentry_sdk.capture_exception(exc)
            error = HordeError.wrap_unhandled(exc)
            event_subtype = _horde_event_subtype_map.get(
                error.reason, SystemEvent.EventSubType.GENERIC_ERROR
            )
            self._state = self._handle_horde_failure(
                comment=str(error),
                event_subtype=event_subtype,
                horde_failure_reason=error.reason,
                horde_failure_context=error.context,
            )
        finally:
            self._send_job_finished_receipt_if_failed()
            self._undo_allowance_if_failed()
            self.miner_client.close()

    def _send_job_finished_receipt_if_failed(self) -> None:
        if self._state == DriverState.COMPLETE:
            return

        if self.miner_client.ws is None:
            return

        try:
            started_timestamp = datetime.datetime.fromtimestamp(self._started_at, datetime.UTC)
            receipt_payload = JobFinishedReceiptPayload(
                job_uuid=str(self.job.job_uuid),
                miner_hotkey=self.miner_hotkey,
                validator_hotkey=self.my_keypair.ss58_address,
                timestamp=datetime.datetime.now(datetime.UTC),
                time_started=started_timestamp,
                time_took_us=int((time.time() - self._started_at) * 1_000_000),
                block_numbers=[],
                score_str="0.000000",
            )
            receipt_signature = sign_blob(self.my_keypair, receipt_payload.blob_for_signing())
            self.miner_client.send(
                V0JobFinishedReceiptRequest(payload=receipt_payload, signature=receipt_signature)
            )
            JobFinishedReceipt.from_payload(
                receipt_payload,
                validator_signature=receipt_signature,
            ).save()
        except Exception as exc:
            logger.warning(
                "Failed to send/persist JobFinishedReceipt for failed job %s: %s",
                self.job.job_uuid,
                exc,
                exc_info=True,
            )

    def _undo_allowance_if_failed(self) -> None:
        if self._state == DriverState.COMPLETE:
            return

        if self.job.allowance_reservation_id is None:
            return

        try:
            allowance().undo_allowance_reservation(self.job.allowance_reservation_id)
            logger.info(
                "Successfully undid allowance reservation %s for failed job %s",
                self.job.allowance_reservation_id,
                self.job.job_uuid,
            )
        except Exception as exc:
            logger.error(
                "Failed to undo allowance reservation %s for failed job %s: %s",
                self.job.allowance_reservation_id,
                self.job.job_uuid,
                exc,
                exc_info=True,
            )

    def _run(self) -> None:
        while True:
            match self._state:
                case DriverState.CONNECT:
                    self._state = self._connect()
                case DriverState.RESERVE_EXECUTOR:
                    self._state = self._reserve_executor()
                case DriverState.WAIT_JOB_ACCEPTED:
                    self._state = self._wait_job_accepted()
                case DriverState.SEND_JOB_ACCEPTED_RECEIPT:
                    self._state = self._send_job_accepted_receipt()
                case DriverState.WAIT_EXECUTOR_READY:
                    self._state = self._wait_executor_ready()
                case DriverState.PREPARE_VOLUMES:
                    self._state = self._prepare_volumes()
                case DriverState.WAIT_VOLUMES_READY:
                    self._state = self._wait_volumes_ready()
                case DriverState.WAIT_STREAMING_JOB_READY:
                    self._state = self._wait_streaming_job_ready()
                case DriverState.WAIT_EXECUTION_DONE:
                    self._state = self._wait_execution_done()
                case DriverState.COLLECT_RESULTS:
                    self._state = self._collect_results()
                case DriverState.COMPLETE:
                    return
                case DriverState.FAILED:
                    return
                case _:
                    assert_never(self._state)

    def _connect(self) -> DriverState:
        assert self._state == DriverState.CONNECT

        try:
            self.miner_client.connect()
        except Exception as exc:
            return self._handle_horde_failure(
                comment=f"Miner connection failed: {exc}",
                event_subtype=SystemEvent.EventSubType.MINER_CONNECTION_ERROR,
                horde_failure_reason=HordeFailureReason.MINER_CONNECTION_FAILED,
                log_exc_info=True,
            )

        return DriverState.RESERVE_EXECUTOR

    def _reserve_executor(self) -> DriverState:
        assert self._state == DriverState.RESERVE_EXECUTOR

        receipt_payload = JobStartedReceiptPayload(
            job_uuid=str(self.job.job_uuid),
            miner_hotkey=self.miner_hotkey,
            validator_hotkey=self.my_keypair.ss58_address,
            timestamp=datetime.datetime.now(datetime.UTC),
            executor_class=self.request.executor_class,
            is_organic=True,
            ttl=self.reservation_time_limit,
        )
        receipt_signature = sign_blob(self.my_keypair, receipt_payload.blob_for_signing())

        executor_timing = V0InitialJobRequest.ExecutorTimingDetails(
            allowed_leeway=self.allowed_leeway,
            download_time_limit=int(self.request.download_time_limit),
            execution_time_limit=int(self.request.execution_time_limit),
            upload_time_limit=int(self.request.upload_time_limit),
            streaming_start_time_limit=int(self.request.streaming_start_time_limit),
        )

        msg = V0InitialJobRequest(
            job_uuid=str(self.job.job_uuid),
            executor_class=self.request.executor_class,
            docker_image=self.request.docker_image,
            timeout_seconds=None,
            volume=self.request.volume,
            job_started_receipt_payload=receipt_payload,
            job_started_receipt_signature=receipt_signature,
            streaming_details=self.request.streaming_details,
            executor_timing=executor_timing,
        )

        try:
            self.miner_client.send(msg)
        except Exception as exc:
            return self._handle_horde_failure(
                comment=f"Failed to send initial job request: {exc}",
                event_subtype=SystemEvent.EventSubType.MINER_SEND_ERROR,
                horde_failure_reason=HordeFailureReason.MINER_CONNECTION_FAILED,
                log_exc_info=True,
            )

        try:
            JobStartedReceipt.from_payload(
                receipt_payload,
                validator_signature=receipt_signature,
            ).save()
        except Exception as exc:
            # Receipt persistence failure should not fail the job.
            logger.warning(
                "Failed to persist JobStartedReceipt for job %s: %s",
                self.job.job_uuid,
                exc,
                exc_info=True,
            )

        return DriverState.WAIT_JOB_ACCEPTED

    def _wait_job_accepted(self) -> DriverState:
        assert self._state == DriverState.WAIT_JOB_ACCEPTED

        result = self._wait_for_message(
            V0AcceptJobRequest,
            timeout=self.reservation_time_limit,
            timeout_status_reason=HordeFailureReason.INITIAL_RESPONSE_TIMED_OUT,
        )
        if isinstance(result, DriverState):
            return result

        self.status_callback(
            JobStatusUpdate(uuid=str(self.job.job_uuid), status=JobStatus.ACCEPTED)
        )

        return DriverState.SEND_JOB_ACCEPTED_RECEIPT

    def _send_job_accepted_receipt(self) -> DriverState:
        assert self._state == DriverState.SEND_JOB_ACCEPTED_RECEIPT

        executor_spinup_time_limit = EXECUTOR_CLASS[self.request.executor_class].spin_up_time
        readiness_time_limit = executor_spinup_time_limit + self.executor_startup_time_limit
        job_accepted_receipt_ttl = (
            readiness_time_limit
            + self.allowed_leeway
            + self.request.download_time_limit
            + self.request.execution_time_limit
            + self.request.upload_time_limit
            + self.request.streaming_start_time_limit
        )
        receipt_payload = JobAcceptedReceiptPayload(
            job_uuid=str(self.job.job_uuid),
            miner_hotkey=self.miner_hotkey,
            validator_hotkey=self.my_keypair.ss58_address,
            timestamp=datetime.datetime.now(datetime.UTC),
            time_accepted=datetime.datetime.now(datetime.UTC),
            ttl=job_accepted_receipt_ttl,
        )
        receipt_signature = sign_blob(self.my_keypair, receipt_payload.blob_for_signing())
        receipt_msg = V0JobAcceptedReceiptRequest(
            payload=receipt_payload, signature=receipt_signature
        )

        try:
            self.miner_client.send(receipt_msg)
            JobAcceptedReceipt.from_payload(
                receipt_payload,
                validator_signature=receipt_signature,
            ).save()
        except Exception as exc:
            comment = (
                f"Failed to send/persist JobAcceptedReceipt for job {self.job.job_uuid}: {exc}"
            )
            logger.warning(comment, exc_info=True)
            self._record_event(
                type=SystemEvent.EventType.RECEIPT_FAILURE,
                subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
                long_description=comment,
            )
            # Receipt failure does NOT fail the job in async version, but here it might.
            # Let's stay consistent with async: it doesn't fail the job.

        return DriverState.WAIT_EXECUTOR_READY

    def _wait_executor_ready(self) -> DriverState:
        assert self._state == DriverState.WAIT_EXECUTOR_READY

        executor_spinup_time_limit = EXECUTOR_CLASS[self.request.executor_class].spin_up_time
        readiness_time_limit = executor_spinup_time_limit + self.executor_startup_time_limit

        result = self._wait_for_message(
            V0ExecutorReadyRequest,
            timeout=readiness_time_limit,
            timeout_status_reason=HordeFailureReason.EXECUTOR_READINESS_RESPONSE_TIMED_OUT,
        )
        if isinstance(result, DriverState):
            return result

        # Initialize the deadline timer with allowed_leeway.
        # Subsequent stages will extend this deadline.
        self._deadline = Timer(self.allowed_leeway)

        self.status_callback(
            JobStatusUpdate(
                uuid=str(self.job.job_uuid),
                status=JobStatus.EXECUTOR_READY,
            )
        )
        return DriverState.PREPARE_VOLUMES

    def _prepare_volumes(self) -> DriverState:
        assert self._state == DriverState.PREPARE_VOLUMES

        has_gpu = EXECUTOR_CLASS[self.request.executor_class].has_gpu
        docker_run_options_preset: DockerRunOptionsPreset = "nvidia_all" if has_gpu else "none"

        msg = V0JobRequest(
            job_uuid=str(self.job.job_uuid),
            executor_class=self.request.executor_class,
            docker_image=self.request.docker_image,
            docker_run_options_preset=docker_run_options_preset,
            docker_run_cmd=self.request.get_args(),
            env=self.request.env,
            volume=None,  # already sent in V0InitialJobRequest
            output_upload=self.request.output_upload,
            artifacts_dir=self.request.artifacts_dir,
        )

        try:
            self.miner_client.send(msg)
        except Exception as exc:
            return self._handle_horde_failure(
                comment=f"Failed to send job request: {exc}",
                event_subtype=SystemEvent.EventSubType.MINER_SEND_ERROR,
                horde_failure_reason=HordeFailureReason.MINER_CONNECTION_FAILED,
                log_exc_info=True,
            )

        return DriverState.WAIT_VOLUMES_READY

    def _wait_volumes_ready(self) -> DriverState:
        assert self._state == DriverState.WAIT_VOLUMES_READY
        assert self._deadline is not None

        self._deadline.extend_timeout(self.request.download_time_limit)

        result = self._wait_for_message(
            V0VolumesReadyRequest,
            timeout=self._deadline.time_left(),
            timeout_status_reason=HordeFailureReason.VOLUMES_TIMED_OUT,
        )
        if isinstance(result, DriverState):
            return result

        self.status_callback(
            JobStatusUpdate(uuid=str(self.job.job_uuid), status=JobStatus.VOLUMES_READY)
        )

        if self.request.streaming_details:
            return DriverState.WAIT_STREAMING_JOB_READY
        else:
            return DriverState.WAIT_EXECUTION_DONE

    def _wait_streaming_job_ready(self) -> DriverState:
        assert self._state == DriverState.WAIT_STREAMING_JOB_READY
        assert self._deadline is not None

        self._deadline.extend_timeout(self.request.streaming_start_time_limit)

        result = self._wait_for_message(
            V0StreamingJobReadyRequest,
            timeout=self._deadline.time_left(),
            timeout_status_reason=HordeFailureReason.STREAMING_JOB_READY_TIMED_OUT,
        )
        if isinstance(result, DriverState):
            return result

        self.status_callback(
            JobStatusUpdate(
                uuid=str(self.job.job_uuid),
                status=JobStatus.STREAMING_READY,
                metadata=JobStatusMetadata(
                    streaming_details=StreamingServerDetails(
                        streaming_server_cert=result.public_key,
                        streaming_server_address=result.ip,
                        streaming_server_port=result.port,
                    )
                ),
            )
        )

        return DriverState.WAIT_EXECUTION_DONE

    def _wait_execution_done(self) -> DriverState:
        assert self._state == DriverState.WAIT_EXECUTION_DONE
        assert self._deadline is not None

        self._deadline.extend_timeout(self.request.execution_time_limit)

        result = self._wait_for_message(
            V0ExecutionDoneRequest,
            timeout=self._deadline.time_left(),
            timeout_status_reason=HordeFailureReason.EXECUTION_TIMED_OUT,
        )
        if isinstance(result, DriverState):
            return result

        self.status_callback(
            JobStatusUpdate(uuid=str(self.job.job_uuid), status=JobStatus.EXECUTION_DONE)
        )
        return DriverState.COLLECT_RESULTS

    def _collect_results(self) -> DriverState:
        assert self._state == DriverState.COLLECT_RESULTS
        assert self._deadline is not None

        self._deadline.extend_timeout(self.request.upload_time_limit)

        result = self._wait_for_message(
            V0JobFinishedRequest,
            timeout=self._deadline.time_left(),
            timeout_status_reason=HordeFailureReason.FINAL_RESPONSE_TIMED_OUT,
        )
        if isinstance(result, DriverState):
            return result

        if self.job.allowance_reservation_id is not None:
            try:
                allowance().spend_allowance(self.job.allowance_reservation_id)
                logger.info(
                    "Successfully spent allowance for reservation %s for job %s",
                    self.job.allowance_reservation_id,
                    self.job.job_uuid,
                )
            except Exception as exc:
                logger.error(
                    "Failed to spend allowance for reservation %s for job %s: %s",
                    self.job.allowance_reservation_id,
                    self.job.job_uuid,
                    exc,
                    exc_info=True,
                )

        self.job.stdout = result.docker_process_stdout
        self.job.stderr = result.docker_process_stderr
        self.job.artifacts = result.artifacts or {}
        self.job.upload_results = result.upload_results or {}
        self.job.status = OrganicJob.Status.COMPLETED
        comment = f"Miner job finished: stdout={len(self.job.stdout)} bytes stderr={len(self.job.stderr)} bytes"
        self.job.comment = comment
        self.job.save(
            update_fields=["stdout", "stderr", "artifacts", "upload_results", "status", "comment"]
        )

        self._record_event(
            type=SystemEvent.EventType.MINER_ORGANIC_JOB_SUCCESS,
            subtype=SystemEvent.EventSubType.SUCCESS,
            long_description=comment,
        )

        try:
            started_timestamp = datetime.datetime.fromtimestamp(self._started_at, datetime.UTC)
            receipt_payload = JobFinishedReceiptPayload(
                job_uuid=str(self.job.job_uuid),
                miner_hotkey=self.miner_hotkey,
                validator_hotkey=self.my_keypair.ss58_address,
                timestamp=datetime.datetime.now(datetime.UTC),
                time_started=started_timestamp,
                time_took_us=int((time.time() - self._started_at) * 1_000_000),
                block_numbers=self.job.allowance_blocks or [],
                score_str=f"{float(self.job.allowance_job_value or 0):.6f}",
            )
            receipt_signature = sign_blob(self.my_keypair, receipt_payload.blob_for_signing())
            self.miner_client.send(
                V0JobFinishedReceiptRequest(payload=receipt_payload, signature=receipt_signature)
            )
            JobFinishedReceipt.from_payload(
                receipt_payload,
                validator_signature=receipt_signature,
            ).save()
        except Exception as exc:
            logger.warning(
                "Failed to send/persist JobFinishedReceipt for job %s: %s",
                self.job.job_uuid,
                exc,
                exc_info=True,
            )

        self.status_callback(
            JobStatusUpdate(
                uuid=str(self.job.job_uuid),
                status=JobStatus.COMPLETED,
                metadata=JobStatusMetadata(
                    miner_response=JobResultDetails(
                        docker_process_stdout=self.job.stdout,
                        docker_process_stderr=self.job.stderr,
                        artifacts=self.job.artifacts,
                        upload_results=self.job.upload_results,
                    )
                ),
            )
        )

        return DriverState.COMPLETE


def execute_organic_job_request_sync(
    job_request: OrganicJobRequest, job_route: JobRoute
) -> OrganicJob:
    if (
        job_route.miner.hotkey_ss58 == settings.DEBUG_MINER_KEY
        and settings.DEBUG_MINER_ADDRESS
        and settings.DEBUG_MINER_PORT
    ):
        miner_ip = settings.DEBUG_MINER_ADDRESS
        miner_port = settings.DEBUG_MINER_PORT
        ip_type = 4
        on_trusted_miner = False
    elif job_route.miner.hotkey_ss58 == TRUSTED_MINER_FAKE_KEY:
        miner_ip = settings.TRUSTED_MINER_ADDRESS
        miner_port = settings.TRUSTED_MINER_PORT
        ip_type = 4
        on_trusted_miner = True
    else:
        miner_ip = job_route.miner.address
        miner_port = job_route.miner.port
        ip_type = job_route.miner.ip_version
        on_trusted_miner = False

    miner = Miner.objects.get(hotkey=job_route.miner.hotkey_ss58)
    job = OrganicJob.objects.create(
        job_uuid=str(job_request.uuid),
        miner=miner,
        miner_address=miner_ip,
        miner_address_ip_version=ip_type,
        miner_port=miner_port,
        namespace=job_request.job_namespace or job_request.docker_image or None,
        executor_class=job_request.executor_class,
        job_description="User job from facilitator",
        block=_get_current_block_sync(),
        on_trusted_miner=on_trusted_miner,
        streaming_details=job_request.streaming_details.model_dump()
        if job_request.streaming_details
        else None,
        allowance_blocks=job_route.allowance_blocks,
        allowance_reservation_id=job_route.allowance_reservation_id,
        allowance_job_value=job_route.allowance_job_value or 0,
    )

    my_wallet: bittensor_wallet.Wallet = settings.BITTENSOR_WALLET()
    my_keypair = my_wallet.hotkey
    ws_url = f"ws://{miner_ip}:{miner_port}/v0.1/validator_interface/{my_keypair.ss58_address}"
    client = MinerClient(
        url=ws_url, miner_hotkey=job_route.miner.hotkey_ss58, validator_keypair=my_keypair
    )

    def status_callback(status_update: JobStatusUpdate) -> None:
        # TODO: use a db table to "transfer" the status updates
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.send)(
            f"job_status_updates__{status_update.uuid}",
            {"type": "job_status_update", "payload": status_update.model_dump(mode="json")},
        )

    driver = SyncOrganicJobDriver(
        client,
        job,
        job_request,
        miner_hotkey=job_route.miner.hotkey_ss58,
        my_keypair=my_keypair,
        allowed_leeway=get_config("DYNAMIC_ORGANIC_JOB_ALLOWED_LEEWAY_TIME"),
        reservation_time_limit=get_config("DYNAMIC_EXECUTOR_RESERVATION_TIME_LIMIT"),
        executor_startup_time_limit=get_config("DYNAMIC_EXECUTOR_STARTUP_TIME_LIMIT"),
        max_overall_time_limit=get_config("DYNAMIC_MAX_OVERALL_ORGANIC_JOB_TIME_LIMIT"),
        status_callback=status_callback,
    )
    driver.run()
    return job
