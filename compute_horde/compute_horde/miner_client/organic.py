import asyncio
import contextlib
import datetime
import enum
import logging
import time
from dataclasses import asdict, dataclass, field
from functools import cached_property

import bittensor
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import OutputUpload
from compute_horde_core.streaming import StreamingDetails
from compute_horde_core.volume import Volume
from pydantic import TypeAdapter

from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde.miner_client.base import (
    AbstractMinerClient,
    ErrorCallback,
    UnsupportedMessageReceived,
)
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
from compute_horde.receipts.models import (
    JobAcceptedReceipt,
    JobFinishedReceipt,
    JobStartedReceipt,
)
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.transport import (
    AbstractTransport,
    TransportConnectionError,
    WSTransport,
)
from compute_horde.utils import MachineSpecs, Timer, sign_blob

logger = logging.getLogger(__name__)


class OrganicMinerClient(AbstractMinerClient[MinerToValidatorMessage, ValidatorToMinerMessage]):
    """
    Miner client to run organic job on a miner.
    This client is used by validators to connect to prod miners.
    Others can use this client to connect to their locally running miners to run jobs.

    The usual usage of this miner client is as follows::

        client = OrganicMinerClient(...)
        async with client:
            await client.send_model(V0InitialJobRequest(...))
            msg = await client.miner_ready_or_declining_future
            # handle msg

            await client.send_model(V0JobRequest(...))
            msg = client.miner_finished_or_failed_future
            # handle msg

    Note that the waiting on the response futures should properly be handled with timeouts
    (with ``asyncio.timeout()``, ``asyncio.wait_for()`` etc.).
    """

    def __init__(
        self,
        miner_hotkey: str,
        miner_address: str,
        miner_port: int,
        job_uuid: str,
        my_keypair: bittensor.Keypair,
        transport: AbstractTransport | None = None,
    ) -> None:
        self.job_uuid = job_uuid

        self.miner_hotkey = miner_hotkey
        self.miner_address = miner_address
        self.miner_port = miner_port
        self.my_keypair = my_keypair

        loop = asyncio.get_running_loop()
        self.miner_manifest = loop.create_future()
        self.online_executor_count = 0

        # for waiting on miner responses
        self.miner_accepting_or_declining_future: asyncio.Future[
            V0AcceptJobRequest | V0DeclineJobRequest
        ] = loop.create_future()
        self.miner_accepting_or_declining_timestamp: int = 0

        self.executor_ready_or_failed_future: asyncio.Future[
            V0ExecutorReadyRequest | V0ExecutorFailedRequest
        ] = loop.create_future()
        self.executor_ready_or_failed_timestamp: int = 0

        self.streaming_job_ready_or_not_future: asyncio.Future[
            V0StreamingJobReadyRequest | V0StreamingJobNotReadyRequest
        ] = loop.create_future()
        self.streaming_job_ready_or_not_timestamp: int = 0

        self.volumes_ready_future: asyncio.Future[V0VolumesReadyRequest | V0JobFailedRequest] = (
            loop.create_future()
        )
        self.volumes_ready_timestamp: int = 0

        self.execution_done_future: asyncio.Future[V0ExecutionDoneRequest | V0JobFailedRequest] = (
            loop.create_future()
        )
        self.execution_done_timestamp: int = 0

        self.miner_finished_or_failed_future: asyncio.Future[
            V0JobFailedRequest | V0JobFinishedRequest
        ] = loop.create_future()
        self.miner_finished_or_failed_timestamp: int = 0

        self.miner_machine_specs: MachineSpecs | None = None

        name = f"{miner_hotkey}({miner_address}:{miner_port})"
        transport = transport or WSTransport(name, self.miner_url())
        super().__init__(name, transport)

    @cached_property
    def my_hotkey(self) -> str:
        return str(self.my_keypair.ss58_address)

    def miner_url(self) -> str:
        return (
            f"ws://{self.miner_address}:{self.miner_port}/v0.1/validator_interface/{self.my_hotkey}"
        )

    def parse_message(self, raw_msg: str | bytes) -> MinerToValidatorMessage:
        return TypeAdapter(MinerToValidatorMessage).validate_json(raw_msg)

    async def notify_generic_error(self, msg: GenericError) -> None:
        """This method is called when miner sends a generic error message"""

    async def notify_unauthorized_error(self, msg: UnauthorizedError) -> None:
        """This method is called when miner refuses the authentication message"""

    async def notify_receipt_failure(self, comment: str) -> None:
        """This method is called when sending receipts to miner fails"""

    async def notify_send_failure(self, msg: str) -> None:
        """This method is called when sending messages to miner fails"""

    async def notify_job_accepted(self, msg: V0AcceptJobRequest) -> None:
        """This method is called when miner sends job accepted message"""

    async def notify_executor_ready(self, msg: V0ExecutorReadyRequest) -> None:
        """This method is called when miner sends executor ready message"""

    async def notify_volumes_ready(self, msg: V0VolumesReadyRequest) -> None:
        """This method is called when miner sends executor ready message"""

    async def notify_execution_done(self, msg: V0ExecutionDoneRequest) -> None:
        """This method is called when miner sends execution done message"""

    async def notify_streaming_readiness(self, msg: V0StreamingJobReadyRequest) -> None:
        """This method is called when miner sends streaming ready message"""

    async def handle_manifest_request(self, msg: V0ExecutorManifestRequest) -> None:
        try:
            self.miner_manifest.set_result(msg.manifest)
        except asyncio.InvalidStateError:
            logger.warning(f"Received manifest from {msg} but future was already set")

    async def handle_machine_specs_request(self, msg: V0MachineSpecsRequest) -> None:
        self.miner_machine_specs = msg.specs

    async def handle_message(self, msg: MinerToValidatorMessage) -> None:
        logger.debug(f"Received message: {msg.message_type}")
        if isinstance(msg, GenericError):
            logger.warning(
                f"Received error message from miner {self.miner_name}: {msg.model_dump_json()}"
            )
            await self.notify_generic_error(msg)
            return
        elif isinstance(msg, UnauthorizedError):
            logger.error(f"Unauthorized in {self.miner_name}: {msg.code}, details: {msg.details}")
            await self.notify_unauthorized_error(msg)
            return
        elif isinstance(msg, V0ExecutorManifestRequest):
            await self.handle_manifest_request(msg)
            return

        if (received_job_uuid := getattr(msg, "job_uuid", self.job_uuid)) != self.job_uuid:
            logger.warning(
                f"Received msg from {self.miner_name} for a different job (expected job {self.job_uuid}, got job {received_job_uuid}): {msg}"
            )
            return

        if isinstance(msg, V0AcceptJobRequest | V0DeclineJobRequest):
            try:
                self.miner_accepting_or_declining_future.set_result(msg)
                self.miner_accepting_or_declining_timestamp = int(time.time())
            except asyncio.InvalidStateError:
                logger.warning(f"Received {msg} from {self.miner_name} but future was already set")
        elif isinstance(msg, V0ExecutorReadyRequest | V0ExecutorFailedRequest):
            try:
                self.executor_ready_or_failed_future.set_result(msg)
                self.executor_ready_or_failed_timestamp = int(time.time())
            except asyncio.InvalidStateError:
                logger.warning(f"Received {msg} from {self.miner_name} but future was already set")
        elif isinstance(msg, V0StreamingJobReadyRequest | V0StreamingJobNotReadyRequest):
            try:
                self.streaming_job_ready_or_not_future.set_result(msg)
                self.streaming_job_ready_or_not_timestamp = int(time.time())
            except asyncio.InvalidStateError:
                logger.warning(f"Received {msg} from {self.miner_name} but future was already set")
        elif isinstance(msg, V0VolumesReadyRequest):
            try:
                self.volumes_ready_future.set_result(msg)
                self.volumes_ready_timestamp = int(time.time())
            except asyncio.InvalidStateError:
                logger.warning(f"Received {msg} from {self.miner_name} but future was already set")
        elif isinstance(msg, V0ExecutionDoneRequest):
            try:
                self.execution_done_future.set_result(msg)
                self.execution_done_timestamp = int(time.time())
            except asyncio.InvalidStateError:
                logger.warning(f"Received {msg} from {self.miner_name} but future was already set")
        elif isinstance(msg, V0JobFinishedRequest):
            try:
                self.miner_finished_or_failed_future.set_result(msg)
                self.miner_finished_or_failed_timestamp = int(time.time())
            except asyncio.InvalidStateError:
                logger.warning(f"Received {msg} from {self.miner_name} but future was already set")
        elif isinstance(msg, V0JobFailedRequest):
            # Consider remaining stages as failed.
            try:
                self.volumes_ready_future.set_result(msg)
                self.volumes_ready_timestamp = int(time.time())
            except asyncio.InvalidStateError:
                pass
            try:
                self.execution_done_future.set_result(msg)
                self.execution_done_timestamp = int(time.time())
            except asyncio.InvalidStateError:
                pass
            try:
                self.miner_finished_or_failed_future.set_result(msg)
                self.miner_finished_or_failed_timestamp = int(time.time())
            except asyncio.InvalidStateError:
                pass
        elif isinstance(msg, V0MachineSpecsRequest):
            await self.handle_machine_specs_request(msg)
        else:
            raise UnsupportedMessageReceived(msg)

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

    async def send_job_accepted_receipt_message(
        self,
        accepted_timestamp: float,
        ttl: int,
    ) -> None:
        try:
            receipt_message = self.generate_job_accepted_receipt_message(
                accepted_timestamp,
                ttl,
            )
            await self.send_model(receipt_message)
            await JobAcceptedReceipt.from_payload(
                receipt_message.payload,
                validator_signature=receipt_message.signature,
            ).asave()
            logger.debug(f"Sent job accepted receipt for {self.job_uuid}")
        except Exception as e:
            comment = f"Failed to send job accepted receipt to miner {self.miner_name} for job {self.job_uuid}: {e}"
            logger.warning(comment)
            await self.notify_receipt_failure(comment)

    def generate_job_finished_receipt_message(
        self,
        started_timestamp: float,
        time_took_seconds: float,
        score: float,
    ) -> V0JobFinishedReceiptRequest:
        time_started = datetime.datetime.fromtimestamp(started_timestamp, datetime.UTC)
        receipt_payload = JobFinishedReceiptPayload(
            job_uuid=self.job_uuid,
            miner_hotkey=self.miner_hotkey,
            validator_hotkey=self.my_hotkey,
            timestamp=datetime.datetime.now(datetime.UTC),
            time_started=time_started,
            time_took_us=int(time_took_seconds * 1_000_000),
            score_str=f"{score:.6f}",
        )
        return V0JobFinishedReceiptRequest(
            payload=receipt_payload,
            signature=sign_blob(self.my_keypair, receipt_payload.blob_for_signing()),
        )

    async def send_job_finished_receipt_message(
        self,
        started_timestamp: float,
        time_took_seconds: float,
        score: float,
    ) -> None:
        try:
            receipt_message = self.generate_job_finished_receipt_message(
                started_timestamp, time_took_seconds, score
            )
            await self.send_model(receipt_message)
            await JobFinishedReceipt.from_payload(
                receipt_message.payload,
                validator_signature=receipt_message.signature,
            ).asave()
            logger.debug(f"Sent job finished receipt for {self.job_uuid}")
        except Exception as e:
            comment = f"Failed to send job finished receipt to miner {self.miner_name} for job {self.job_uuid}: {e}"
            logger.warning(comment)
            await self.notify_receipt_failure(comment)

    async def send_initial_job_request(
        self,
        job_details: "OrganicJobDetails",
        receipt_payload: JobStartedReceiptPayload,
        receipt_signature: str,
    ) -> None:
        await self.send_model(
            V0InitialJobRequest(
                job_uuid=job_details.job_uuid,
                executor_class=job_details.executor_class,
                docker_image=job_details.docker_image,
                volume=job_details.volume,
                job_started_receipt_payload=receipt_payload,
                job_started_receipt_signature=receipt_signature,
                timeout_seconds=job_details.total_job_timeout,
                executor_timing=V0InitialJobRequest.ExecutorTimingDetails(
                    allowed_leeway=job_details.job_timing.allowed_leeway,
                    download_time_limit=job_details.job_timing.download_time_limit,
                    execution_time_limit=job_details.job_timing.execution_time_limit,
                    upload_time_limit=job_details.job_timing.upload_time_limit,
                    streaming_start_time_limit=job_details.job_timing.streaming_start_time_limit,
                )
                if job_details.job_timing is not None
                else None,
                streaming_details=job_details.streaming_details,
            ),
        )

    async def send_model(
        self, model: ValidatorToMinerMessage, error_event_callback: ErrorCallback | None = None
    ) -> None:
        if error_event_callback is None:
            error_event_callback = self.notify_send_failure

        await super().send_model(model, error_event_callback)

    async def connect(self) -> None:
        await super().connect()
        await self.transport.send(self.generate_authentication_message().model_dump_json())


class FailureReason(enum.Enum):
    MINER_CONNECTION_FAILED = enum.auto()
    INITIAL_RESPONSE_TIMED_OUT = enum.auto()
    EXECUTOR_READINESS_RESPONSE_TIMED_OUT = enum.auto()
    VOLUMES_TIMED_OUT = enum.auto()
    VOLUMES_FAILED = enum.auto()
    EXECUTION_TIMED_OUT = enum.auto()
    EXECUTION_FAILED = enum.auto()
    FINAL_RESPONSE_TIMED_OUT = enum.auto()
    JOB_DECLINED = enum.auto()
    EXECUTOR_FAILED = enum.auto()
    STREAMING_JOB_READY_TIMED_OUT = enum.auto()
    JOB_FAILED = enum.auto()
    STREAMING_FAILED = enum.auto()


class OrganicJobError(Exception):
    def __init__(self, reason: FailureReason, received: MinerToValidatorMessage | None = None):
        self.reason = reason
        self.received = received

    def __str__(self):
        s = f"Organic job failed, {self.reason=}"
        if self.received:
            s += f", received: {self.received_str()}"
        return s

    def __repr__(self):
        return f"{type(self).__name__}: {str(self)}"

    def received_str(self) -> str:
        if not self.received:
            return ""
        return self.received.model_dump_json()


@dataclass
class OrganicJobDetails:
    @dataclass
    class TimingDetails:
        allowed_leeway: int
        download_time_limit: int
        execution_time_limit: int
        upload_time_limit: int
        streaming_start_time_limit: int

        @property
        def total(self):
            return sum(asdict(self).values())

    job_uuid: str
    executor_class: ExecutorClass
    docker_image: str
    job_timing: TimingDetails | None = None
    docker_run_options_preset: DockerRunOptionsPreset = "nvidia_all"
    docker_run_cmd: list[str] = field(default_factory=list)
    total_job_timeout: int = 300  # Deprecated, use job_timing instead.
    volume: Volume | None = None
    output: OutputUpload | None = None
    artifacts_dir: str | None = None
    streaming_details: StreamingDetails | None = None


async def execute_organic_job_on_miner(
    client: OrganicMinerClient,
    job_details: OrganicJobDetails,
    reservation_time_limit: int,
    executor_startup_time_limit: int,
) -> tuple[str, str, dict[str, str], dict[str, str]]:  # stdout, stderr, artifacts, upload_results
    """
    Run an organic job. This is a simpler way to use OrganicMinerClient.
    :param client: the organic miner client
    :param job_details: details specific to the job that needs to be run
    :param reservation_time_limit: time for the miner to report reservation success (or decline the job)
    :param executor_startup_time_limit: time for executor to perform startup checks
    :return: standard out, standard error of the job container, artifacts and a dictionary mapping file names to HTTP upload responses
    """
    assert client.job_uuid == job_details.job_uuid

    timer = Timer()  # Only used for measurement, not used for timeouts.

    executor_spinup_time_limit = EXECUTOR_CLASS[job_details.executor_class].spin_up_time
    readiness_time_limit = executor_spinup_time_limit + executor_startup_time_limit

    # Note: If this is empty, we're in single-timeout mode.
    executor_timing = job_details.job_timing

    async with contextlib.AsyncExitStack() as exit_stack:
        try:
            await exit_stack.enter_async_context(client)
        except TransportConnectionError as exc:
            raise OrganicJobError(FailureReason.MINER_CONNECTION_FAILED) from exc

        ## STAGE: reservation
        # Miner should reserve an executor and respond with accept/reject quickly.
        # The receipt should only be good enough for the reservation time.
        # Miner will receive a "job accepted" receipt with a longer TTL as soon as we get a reservation.
        receipt_payload, receipt_signature = client.generate_job_started_receipt_message(
            executor_class=job_details.executor_class,
            ttl=reservation_time_limit,
        )
        await client.send_initial_job_request(job_details, receipt_payload, receipt_signature)
        logger.debug("Sent initial job request")
        await JobStartedReceipt.from_payload(
            receipt_payload,
            validator_signature=receipt_signature,
        ).asave()

        try:
            try:
                logger.debug(f"Waiting for initial response for {reservation_time_limit:.2f}s")
                initial_response = await asyncio.wait_for(
                    client.miner_accepting_or_declining_future,
                    timeout=reservation_time_limit,
                )
            except TimeoutError as exc:
                raise OrganicJobError(FailureReason.INITIAL_RESPONSE_TIMED_OUT) from exc
            if isinstance(initial_response, V0DeclineJobRequest):
                raise OrganicJobError(FailureReason.JOB_DECLINED, initial_response)

            await client.notify_job_accepted(initial_response)

            ## STAGE: Spinup
            # Validator waits for the executor to start up and report readiness.
            # This includes executor startup checks.
            if executor_timing:
                job_accepted_receipt_ttl = readiness_time_limit + executor_timing.total
            else:
                job_accepted_receipt_ttl = readiness_time_limit + job_details.total_job_timeout
            try:
                await client.send_job_accepted_receipt_message(
                    accepted_timestamp=time.time(),
                    ttl=job_accepted_receipt_ttl,
                )
                executor_readiness_response = await asyncio.wait_for(
                    client.executor_ready_or_failed_future,
                    timeout=readiness_time_limit,
                )
            except TimeoutError as exc:
                raise OrganicJobError(FailureReason.EXECUTOR_READINESS_RESPONSE_TIMED_OUT) from exc
            if isinstance(executor_readiness_response, V0ExecutorFailedRequest):
                raise OrganicJobError(FailureReason.EXECUTOR_FAILED, executor_readiness_response)

            await client.notify_executor_ready(executor_readiness_response)

            if executor_timing is not None:
                # For fine-grained timeouts, the deadline will be extended at the start of each stage.
                logger.debug(f"Starting deadline with {executor_timing.allowed_leeway}s leeway")
                deadline = Timer(executor_timing.allowed_leeway)
            else:
                # For single-timeout mode, the deadline is set once here.
                logger.debug(
                    f"Starting deadline with {job_details.total_job_timeout}s total timeout"
                )
                deadline = Timer(job_details.total_job_timeout)

            await client.send_model(
                V0JobRequest(
                    job_uuid=job_details.job_uuid,
                    executor_class=job_details.executor_class,
                    docker_image=job_details.docker_image,
                    docker_run_options_preset=job_details.docker_run_options_preset,
                    docker_run_cmd=job_details.docker_run_cmd,
                    volume=None,  # Was sent in the initial request
                    output_upload=job_details.output,
                    artifacts_dir=job_details.artifacts_dir,
                )
            )

            ## STAGE: Volume download
            try:
                if executor_timing:
                    logger.debug(
                        f"Extending deadline by download_time_limit: +{executor_timing.download_time_limit}s"
                    )
                    deadline.extend_timeout(executor_timing.download_time_limit)
                logger.debug(
                    f"Waiting for volume download (time left: {deadline.time_left():.2f}s)"
                )
                volumes_ready_response = await asyncio.wait_for(
                    client.volumes_ready_future,
                    timeout=deadline.time_left(),
                )
                if isinstance(volumes_ready_response, V0JobFailedRequest):
                    raise OrganicJobError(FailureReason.VOLUMES_FAILED, volumes_ready_response)
                logger.debug(f"Volume download done with {deadline.time_left():.2f}s left")
            except TimeoutError as exc:
                raise OrganicJobError(FailureReason.VOLUMES_TIMED_OUT) from exc
            await client.notify_volumes_ready(volumes_ready_response)

            ## STAGE: Start streaming
            if job_details.streaming_details:
                try:
                    if executor_timing:
                        logger.debug(
                            f"Extending deadline by streaming_start_time_limit: +{executor_timing.streaming_start_time_limit}s"
                        )
                        deadline.extend_timeout(executor_timing.streaming_start_time_limit)
                    logger.debug(f"Waiting for streaming (time left: {deadline.time_left():.2f}s)")
                    streaming_response = await asyncio.wait_for(
                        client.streaming_job_ready_or_not_future,
                        timeout=deadline.time_left(),
                    )
                except TimeoutError as exc:
                    raise OrganicJobError(FailureReason.STREAMING_JOB_READY_TIMED_OUT) from exc
                if isinstance(streaming_response, V0StreamingJobNotReadyRequest):
                    raise OrganicJobError(FailureReason.STREAMING_FAILED, streaming_response)

                await client.notify_streaming_readiness(streaming_response)

            ## STAGE: execution
            try:
                if executor_timing:
                    logger.debug(
                        f"Extending deadline by execution_time_limit: +{executor_timing.execution_time_limit}s"
                    )
                    deadline.extend_timeout(executor_timing.execution_time_limit)
                logger.debug(f"Waiting for execution (time left: {deadline.time_left():.2f}s)")
                execution_done_response = await asyncio.wait_for(
                    client.execution_done_future,
                    timeout=deadline.time_left(),
                )
                if isinstance(execution_done_response, V0JobFailedRequest):
                    raise OrganicJobError(FailureReason.EXECUTION_FAILED, execution_done_response)
                logger.debug(f"Execution done with {deadline.time_left():.2f}s left")
            except TimeoutError as exc:
                raise OrganicJobError(FailureReason.EXECUTION_TIMED_OUT) from exc
            await client.notify_execution_done(execution_done_response)

            ## STAGE: upload
            try:
                if executor_timing:
                    logger.debug(
                        f"Extending deadline by upload_time_limit: +{executor_timing.upload_time_limit}s"
                    )
                    deadline.extend_timeout(executor_timing.upload_time_limit)
                logger.debug(f"Waiting for upload (time left: {deadline.time_left():.2f}s)")
                final_response = await asyncio.wait_for(
                    client.miner_finished_or_failed_future,
                    timeout=deadline.time_left(),
                )
                if isinstance(final_response, V0JobFailedRequest):
                    raise OrganicJobError(FailureReason.JOB_FAILED, final_response)
                logger.debug(f"Upload done with {deadline.time_left():.2f}s left")

                logger.info(f"Job finished in time with {deadline.time_left():.2f}s left")
                await client.send_job_finished_receipt_message(
                    started_timestamp=timer.start_time.timestamp(),
                    time_took_seconds=timer.passed_time(),
                    score=0,  # no score for organic jobs (at least right now)
                )

                return (
                    final_response.docker_process_stdout,
                    final_response.docker_process_stderr,
                    final_response.artifacts or {},
                    final_response.upload_results or {},
                )
            except TimeoutError as exc:
                raise OrganicJobError(FailureReason.FINAL_RESPONSE_TIMED_OUT) from exc

        except Exception as e:
            logger.warning(f"Job failed with {type(e).__name__}: {e}")
            await client.send_job_finished_receipt_message(
                started_timestamp=timer.start_time.timestamp(),
                time_took_seconds=timer.passed_time(),
                score=0,
            )
            raise

    raise Exception("Organic job flow ended with no result")
