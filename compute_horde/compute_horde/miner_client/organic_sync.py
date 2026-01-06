import contextlib
import datetime
import logging
import time
from dataclasses import asdict

import bittensor
from compute_horde_core.executor_class import ExecutorClass
from pydantic import TypeAdapter

from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde.job_errors import HordeError
from compute_horde.protocol_consts import HordeFailureReason, JobParticipantType
from compute_horde.protocol_messages import (
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
    V0MachineSpecsRequest,
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
from compute_horde.utils import MachineSpecs, Timer, sign_blob

logger = logging.getLogger(__name__)


class MinerRejectedJob(Exception):
    def __init__(self, msg: V0DeclineJobRequest):
        self.msg = msg


class MinerReportedJobFailed(Exception):
    def __init__(self, msg: V0JobFailedRequest):
        self.msg = msg


class MinerReportedHordeFailed(Exception):
    def __init__(self, msg: V0HordeFailedRequest):
        self.msg = msg


class MinerConnectionFailed(HordeError):
    def __init__(self, message: str):
        super().__init__(
            reason=HordeFailureReason.MINER_CONNECTION_FAILED,
            message=message,
        )


class MinerTimedOut(HordeError):
    def __init__(self, reason: HordeFailureReason):
        super().__init__(
            reason=reason,
            message=f"Timed out while waiting for miner message ({reason})",
        )


class SyncOrganicMinerClient:
    def __init__(
        self,
        miner_hotkey: str,
        miner_address: str,
        miner_port: int,
        job_uuid: str,
        my_keypair: bittensor.Keypair,
        *,
        max_retries: int = 5,
        base_retry_delay: float = 1.0,
        retry_jitter: float = 1.0,
    ) -> None:
        self.miner_hotkey = miner_hotkey
        self.miner_address = miner_address
        self.miner_port = miner_port
        self.job_uuid = job_uuid
        self.my_keypair = my_keypair

        self.max_retries = max_retries
        self.base_retry_delay = base_retry_delay
        self.retry_jitter = retry_jitter

        self._ws = None
        self.miner_manifest: dict[ExecutorClass, int] | None = None
        self.miner_machine_specs: MachineSpecs | None = None

        self.job_accepted_timestamp: int | None = None
        self.executor_ready_timestamp: int | None = None
        self.streaming_ready_timestamp: int | None = None
        self.volumes_ready_timestamp: int | None = None
        self.execution_done_timestamp: int | None = None
        self.job_finished_timestamp: int | None = None

        self.notify_job_accepted = lambda _msg: None
        self.notify_executor_ready = lambda _msg: None
        self.notify_volumes_ready = lambda _msg: None
        self.notify_execution_done = lambda _msg: None
        self.notify_streaming_readiness = lambda _msg: None

    @property
    def my_hotkey(self) -> str:
        return self.my_keypair.ss58_address

    @property
    def miner_name(self) -> str:
        return f"{self.miner_hotkey}@{self.miner_address}:{self.miner_port}"

    def miner_url(self) -> str:
        return f"ws://{self.miner_address}:{self.miner_port}/ws/v0/validator"  # matches async client

    def notify_generic_error(self, _msg: GenericError) -> None:
        return

    def notify_unauthorized_error(self, _msg: UnauthorizedError) -> None:
        return

    def notify_receipt_failure(self, _comment: str) -> None:
        return

    def notify_send_failure(self, _msg: str) -> None:
        return

    def generate_authentication_message(self) -> ValidatorAuthForMiner:
        msg = ValidatorAuthForMiner(
            validator_hotkey=self.my_hotkey,
            miner_hotkey=self.miner_hotkey,
            timestamp=int(time.time()),
            signature="",
        )
        msg.signature = sign_blob(self.my_keypair, msg.blob_for_signing())
        return msg

    def generate_job_started_receipt_message(
        self,
        executor_class: ExecutorClass,
        ttl: int,
    ) -> tuple[JobStartedReceiptPayload, str]:
        receipt_payload = JobStartedReceiptPayload(
            job_uuid=self.job_uuid,
            miner_hotkey=self.miner_hotkey,
            validator_hotkey=self.my_hotkey,
            timestamp=datetime.datetime.now(datetime.UTC),
            executor_class=executor_class,
            is_organic=True,
            ttl=ttl,
        )
        signature = sign_blob(self.my_keypair, receipt_payload.blob_for_signing())
        return receipt_payload, signature

    def generate_job_accepted_receipt_message(
        self,
        accepted_timestamp: float,
        ttl: int,
    ) -> V0JobAcceptedReceiptRequest:
        time_accepted = datetime.datetime.fromtimestamp(accepted_timestamp, datetime.UTC)
        receipt_payload = JobAcceptedReceiptPayload(
            job_uuid=self.job_uuid,
            miner_hotkey=self.miner_hotkey,
            validator_hotkey=self.my_hotkey,
            timestamp=datetime.datetime.now(datetime.UTC),
            time_accepted=time_accepted,
            ttl=ttl,
        )
        return V0JobAcceptedReceiptRequest(
            payload=receipt_payload,
            signature=sign_blob(self.my_keypair, receipt_payload.blob_for_signing()),
        )

    def send_job_accepted_receipt_message(
        self,
        accepted_timestamp: float,
        ttl: int,
    ) -> None:
        try:
            receipt_message = self.generate_job_accepted_receipt_message(
                accepted_timestamp,
                ttl,
            )
            self.send_model(receipt_message)
            JobAcceptedReceipt.from_payload(
                receipt_message.payload,
                validator_signature=receipt_message.signature,
            ).save()
            logger.debug("Sent job accepted receipt for %s", self.job_uuid)
        except Exception as e:
            comment = (
                f"Failed to send job accepted receipt to miner {self.miner_name} for job {self.job_uuid}: {e}"
            )
            logger.warning(comment)
            self.notify_receipt_failure(comment)

    def generate_job_finished_receipt_message(
        self,
        started_timestamp: float,
        time_took_seconds: float,
        score: float,
        allowance_blocks: list[int],
    ) -> V0JobFinishedReceiptRequest:
        time_started = datetime.datetime.fromtimestamp(started_timestamp, datetime.UTC)
        receipt_payload = JobFinishedReceiptPayload(
            job_uuid=self.job_uuid,
            miner_hotkey=self.miner_hotkey,
            validator_hotkey=self.my_hotkey,
            timestamp=datetime.datetime.now(datetime.UTC),
            time_started=time_started,
            time_took_us=int(time_took_seconds * 1_000_000),
            block_numbers=allowance_blocks,
            score_str=f"{score:.6f}",
        )
        return V0JobFinishedReceiptRequest(
            payload=receipt_payload,
            signature=sign_blob(self.my_keypair, receipt_payload.blob_for_signing()),
        )

    def send_job_finished_receipt_message(
        self,
        started_timestamp: float,
        time_took_seconds: float,
        score: float,
        allowance_blocks: list[int],
    ) -> None:
        try:
            receipt_message = self.generate_job_finished_receipt_message(
                started_timestamp, time_took_seconds, score, allowance_blocks
            )
            self.send_model(receipt_message)
            JobFinishedReceipt.from_payload(
                receipt_message.payload,
                validator_signature=receipt_message.signature,
            ).save()
            logger.debug("Sent job finished receipt for %s", self.job_uuid)
        except Exception as e:
            comment = (
                f"Failed to send job finished receipt to miner {self.miner_name} for job {self.job_uuid}: {e}"
            )
            logger.warning(comment)
            self.notify_receipt_failure(comment)

    def connect(self) -> None:
        from websockets.sync.client import connect  # type: ignore[import-not-found]

        attempt = 0
        last_exc: Exception | None = None
        while self.max_retries == 0 or attempt < self.max_retries:
            try:
                self._ws = connect(self.miner_url(), max_size=50 * (2**20), ping_timeout=120)
                self._ws.send(self.generate_authentication_message().model_dump_json())
                return
            except Exception as exc:
                last_exc = exc
                attempt += 1
                delay = self.base_retry_delay * 2**attempt
                time.sleep(delay)

        raise MinerConnectionFailed(str(last_exc) if last_exc else "Unknown connection error")

    def close(self) -> None:
        if self._ws is None:
            return
        with contextlib.suppress(Exception):
            self._ws.close()
        self._ws = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def send(self, data: str | bytes) -> None:
        if self._ws is None:
            raise RuntimeError("WebSocket connection has not been established")
        try:
            self._ws.send(data)
        except Exception as exc:
            msg = f"Could not send to miner {self.miner_name}: {exc}"
            logger.warning(msg)
            self.notify_send_failure(msg)
            raise MinerConnectionFailed(msg) from exc

    def send_model(self, model: ValidatorToMinerMessage) -> None:
        self.send(model.model_dump_json())

    def receive(self, timeout_seconds: float | None) -> str | bytes:
        if self._ws is None:
            raise RuntimeError("WebSocket connection has not been established")
        try:
            return self._ws.recv(timeout=timeout_seconds)
        except TimeoutError as exc:
            raise exc
        except Exception as exc:
            raise MinerConnectionFailed(str(exc)) from exc

    def receive_model(self, timeout_seconds: float | None) -> MinerToValidatorMessage:
        raw_msg = self.receive(timeout_seconds)
        return TypeAdapter(MinerToValidatorMessage).validate_json(raw_msg)

    def send_initial_job_request(
        self,
        job_details: "OrganicJobDetails",
        receipt_payload: JobStartedReceiptPayload,
        receipt_signature: str,
    ) -> None:
        self.send_model(
            V0InitialJobRequest(
                job_uuid=job_details.job_uuid,
                executor_class=job_details.executor_class,
                docker_image=job_details.docker_image,
                timeout_seconds=job_details.total_job_timeout,
                volume=job_details.volume,
                job_started_receipt_payload=receipt_payload,
                job_started_receipt_signature=receipt_signature,
                executor_timing=V0InitialJobRequest.ExecutorTimingDetails(**asdict(job_details.job_timing))
                if job_details.job_timing is not None
                else None,
                streaming_details=job_details.streaming_details,
            )
        )


def _deadline_time_left(deadline: Timer) -> float:
    left = deadline.time_left()
    if left < 0:
        return 0
    return left


def _recv_until(
    client: SyncOrganicMinerClient,
    *,
    timeout_seconds: float,
) -> MinerToValidatorMessage:
    return client.receive_model(timeout_seconds)


def _handle_incoming(client: SyncOrganicMinerClient, msg: MinerToValidatorMessage) -> MinerToValidatorMessage:
    if isinstance(msg, GenericError):
        logger.warning("Received error message from miner %s: %s", client.miner_name, msg)
        client.notify_generic_error(msg)
        return msg

    if isinstance(msg, UnauthorizedError):
        logger.error("Unauthorized in %s: %s", client.miner_name, msg)
        client.notify_unauthorized_error(msg)
        return msg

    if isinstance(msg, V0ExecutorManifestRequest):
        client.miner_manifest = msg.manifest
        return msg

    if isinstance(msg, V0MachineSpecsRequest):
        client.miner_machine_specs = msg.specs
        return msg

    received_job_uuid = getattr(msg, "job_uuid", client.job_uuid)
    if received_job_uuid != client.job_uuid:
        logger.warning(
            "Received msg from %s for different job (expected %s, got %s): %s",
            client.miner_name,
            client.job_uuid,
            received_job_uuid,
            msg,
        )
        return msg

    if isinstance(msg, V0AcceptJobRequest):
        client.job_accepted_timestamp = int(time.time())
    elif isinstance(msg, V0ExecutorReadyRequest):
        client.executor_ready_timestamp = int(time.time())
    elif isinstance(msg, V0StreamingJobReadyRequest):
        client.streaming_ready_timestamp = int(time.time())
    elif isinstance(msg, V0VolumesReadyRequest):
        client.volumes_ready_timestamp = int(time.time())
    elif isinstance(msg, V0ExecutionDoneRequest):
        client.execution_done_timestamp = int(time.time())
    elif isinstance(msg, V0JobFinishedRequest):
        client.job_finished_timestamp = int(time.time())
    elif isinstance(msg, V0StreamingJobNotReadyRequest):
        mapped_msg = V0HordeFailedRequest(
            job_uuid=msg.job_uuid,
            reported_by=JobParticipantType.MINER,
            reason=HordeFailureReason.STREAMING_FAILED,
            message="Executor reported legacy V0StreamingJobNotReadyRequest message",
        )
        return _handle_incoming(client, mapped_msg)
    elif isinstance(msg, V0ExecutorFailedRequest):
        mapped_msg = V0HordeFailedRequest(
            job_uuid=msg.job_uuid,
            reported_by=JobParticipantType.MINER,
            reason=HordeFailureReason.GENERIC_ERROR,
            message="Executor reported legacy V0ExecutorFailedRequest message",
        )
        return _handle_incoming(client, mapped_msg)
    elif isinstance(msg, V0DeclineJobRequest):
        raise MinerRejectedJob(msg)
    elif isinstance(msg, V0JobFailedRequest):
        raise MinerReportedJobFailed(msg)
    elif isinstance(msg, V0HordeFailedRequest):
        raise MinerReportedHordeFailed(msg)

    return msg


def execute_organic_job_on_miner_sync(
    client: SyncOrganicMinerClient,
    job_details: "OrganicJobDetails",
    reservation_time_limit: int,
    executor_startup_time_limit: int,
) -> tuple[str, str, dict[str, str], dict[str, str]]:
    assert client.job_uuid == job_details.job_uuid

    timer = Timer()

    executor_spinup_time_limit = EXECUTOR_CLASS[job_details.executor_class].spin_up_time
    readiness_time_limit = executor_spinup_time_limit + executor_startup_time_limit
    executor_timing = job_details.job_timing

    with client:
        receipt_payload, receipt_signature = client.generate_job_started_receipt_message(
            executor_class=job_details.executor_class,
            ttl=reservation_time_limit,
        )
        client.send_initial_job_request(job_details, receipt_payload, receipt_signature)
        JobStartedReceipt.from_payload(
            receipt_payload,
            validator_signature=receipt_signature,
        ).save()

        try:
            try:
                msg = _handle_incoming(
                    client,
                    _recv_until(client, timeout_seconds=reservation_time_limit),
                )
                while not isinstance(msg, V0AcceptJobRequest):
                    msg = _handle_incoming(
                        client,
                        _recv_until(client, timeout_seconds=reservation_time_limit),
                    )
            except TimeoutError as exc:
                raise MinerTimedOut(HordeFailureReason.INITIAL_RESPONSE_TIMED_OUT) from exc

            client.notify_job_accepted(msg)

            if executor_timing:
                job_accepted_receipt_ttl = readiness_time_limit + executor_timing.total
            else:
                job_accepted_receipt_ttl = readiness_time_limit + job_details.total_job_timeout

            try:
                client.send_job_accepted_receipt_message(
                    accepted_timestamp=time.time(),
                    ttl=job_accepted_receipt_ttl,
                )

                start = time.monotonic()
                while True:
                    remaining = readiness_time_limit - (time.monotonic() - start)
                    if remaining <= 0:
                        raise MinerTimedOut(HordeFailureReason.EXECUTOR_READINESS_RESPONSE_TIMED_OUT)
                    m = _handle_incoming(client, client.receive_model(remaining))
                    if isinstance(m, V0ExecutorReadyRequest):
                        executor_ready_response = m
                        break
            except TimeoutError as exc:
                raise MinerTimedOut(HordeFailureReason.EXECUTOR_READINESS_RESPONSE_TIMED_OUT) from exc

            client.notify_executor_ready(executor_ready_response)

            if executor_timing is not None:
                deadline = Timer(executor_timing.allowed_leeway)
            else:
                deadline = Timer(job_details.total_job_timeout)

            client.send_model(
                V0JobRequest(
                    job_uuid=job_details.job_uuid,
                    executor_class=job_details.executor_class,
                    docker_image=job_details.docker_image,
                    docker_run_options_preset=job_details.docker_run_options_preset,
                    docker_run_cmd=job_details.docker_run_cmd,
                    env=job_details.env,
                    volume=None,
                    output_upload=job_details.output,
                    artifacts_dir=job_details.artifacts_dir,
                )
            )

            try:
                if executor_timing:
                    deadline.extend_timeout(executor_timing.download_time_limit)
                while True:
                    m = _handle_incoming(client, client.receive_model(_deadline_time_left(deadline)))
                    if isinstance(m, V0VolumesReadyRequest):
                        volumes_ready_response = m
                        break
            except TimeoutError as exc:
                raise MinerTimedOut(HordeFailureReason.VOLUMES_TIMED_OUT) from exc

            client.notify_volumes_ready(volumes_ready_response)

            if job_details.streaming_details:
                try:
                    if executor_timing:
                        deadline.extend_timeout(executor_timing.streaming_start_time_limit)
                    while True:
                        m = _handle_incoming(
                            client, client.receive_model(_deadline_time_left(deadline))
                        )
                        if isinstance(m, V0StreamingJobReadyRequest):
                            streaming_response = m
                            break
                except TimeoutError as exc:
                    raise MinerTimedOut(HordeFailureReason.STREAMING_JOB_READY_TIMED_OUT) from exc

                client.notify_streaming_readiness(streaming_response)

            try:
                if executor_timing:
                    deadline.extend_timeout(executor_timing.execution_time_limit)
                while True:
                    m = _handle_incoming(client, client.receive_model(_deadline_time_left(deadline)))
                    if isinstance(m, V0ExecutionDoneRequest):
                        execution_done_response = m
                        break
            except TimeoutError as exc:
                raise MinerTimedOut(HordeFailureReason.EXECUTION_TIMED_OUT) from exc

            client.notify_execution_done(execution_done_response)

            try:
                if executor_timing:
                    deadline.extend_timeout(executor_timing.upload_time_limit)
                while True:
                    m = _handle_incoming(client, client.receive_model(_deadline_time_left(deadline)))
                    if isinstance(m, V0JobFinishedRequest):
                        final_response = m
                        break

                client.send_job_finished_receipt_message(
                    started_timestamp=timer.start_time.timestamp(),
                    time_took_seconds=timer.passed_time(),
                    score=job_details.allowance_job_value,
                    allowance_blocks=job_details.allowance_blocks,
                )

                return (
                    final_response.docker_process_stdout,
                    final_response.docker_process_stderr,
                    final_response.artifacts or {},
                    final_response.upload_results or {},
                )
            except TimeoutError as exc:
                raise MinerTimedOut(HordeFailureReason.FINAL_RESPONSE_TIMED_OUT) from exc

        except Exception as e:
            logger.warning("Job failed with %s: %s", type(e).__name__, e)
            client.send_job_finished_receipt_message(
                started_timestamp=timer.start_time.timestamp(),
                time_took_seconds=timer.passed_time(),
                score=0,
                allowance_blocks=[],
            )
            raise
