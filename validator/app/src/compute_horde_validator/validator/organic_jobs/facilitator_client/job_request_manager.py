import asyncio

import pydantic
import sentry_sdk
from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest, V0JobCheated
from compute_horde.fv_protocol.validator_requests import (
    HordeFailureDetails,
    JobRejectionDetails,
    JobStatusMetadata,
    JobStatusUpdate,
)
from compute_horde.job_errors import HordeError
from compute_horde.protocol_consts import (
    HordeFailureReason,
    JobParticipantType,
    JobRejectionReason,
    JobStatus,
)
from compute_horde.protocol_messages import FailureContext

from compute_horde_validator.validator.allowance.types import NotEnoughAllowanceException
from compute_horde_validator.validator.models import SystemEvent
from compute_horde_validator.validator.routing.types import JobRoutingException

from .base import BaseComponent
from .constants import (
    CHEATED_JOB_REPORT_CHANNEL,
    JOB_REQUEST_CHANNEL,
    JOB_STATUS_UPDATE_CHANNEL,
    WAIT_ON_ERROR_INTERVAL,
)
from .exceptions import LocalChannelReceiveError
from .jobs_task import JobRequestVerificationFailed, job_request_task, process_miner_cheat_report
from .metrics import VALIDATOR_FC_COMPONENT_STATE
from .util import (
    interruptible_receive_local_message,
    interruptible_wait,
    log_system_error_event,
    safe_send_local_message,
    stop_task_gracefully,
)


class JobRequestManager(BaseComponent):
    """
    Handles job requests and cheated job reports sent from the facilitator.
    """

    def __init__(self) -> None:
        super().__init__()
        self._job_request_listener_task: asyncio.Task[None] | None = None
        self._cheated_job_report_listener_task: asyncio.Task[None] | None = None

    async def _job_request_handler(self) -> None:
        """
        Listens for messages on the local job requests channel and forwards these to the job dispatcher.
        """
        while self.is_running():
            try:
                msg_or_none = await interruptible_receive_local_message(
                    JOB_REQUEST_CHANNEL, stop_event=self._stop_event
                )
                if msg_or_none is not None:
                    job_request: OrganicJobRequest = OrganicJobRequest.model_validate(msg_or_none)
                    # job_request_task.delay(job_request.model_dump_json())
                    await job_request_task(job_request.model_dump_json())
            except asyncio.CancelledError:
                self._stop_event.set()
                VALIDATOR_FC_COMPONENT_STATE.labels(component=self.name).set(0)
                break
            except pydantic.ValidationError:
                msg = f"Invalid job request received from facilitator: {msg_or_none}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except LocalChannelReceiveError as exc:
                self._logger.error(str(exc))
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_RECEIVE_ERROR,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except JobRequestVerificationFailed as exc:
                self._logger.error(
                    f"Job request verification failed: {job_request.model_dump_json()}"
                )
                await self._send_job_rejected_message(
                    job_uuid=job_request.uuid,
                    message=exc.message,
                    rejected_by=JobParticipantType.VALIDATOR,
                    reason=JobRejectionReason.INVALID_SIGNATURE,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except (NotEnoughAllowanceException, JobRoutingException) as exc:
                self._logger.error(
                    f"Job could not be routed to a miner ({type(exc).__qualname__}): {job_request.model_dump_json()}"
                )
                await self._send_job_rejected_message(
                    job_uuid=job_request.uuid,
                    message="Job could not be routed to a miner",
                    rejected_by=JobParticipantType.VALIDATOR,
                    reason=JobRejectionReason.NO_MINER_FOR_JOB,
                    context={"exception_type": type(exc).__qualname__},
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                self._logger.error(f"Error handling job request: {type(exc).__name__}: {exc}")
                wrapped_exc = HordeError.wrap_unhandled(exc)
                await self._make_horde_failed_message(
                    job_uuid=job_request.uuid,
                    reported_by=JobParticipantType.VALIDATOR,
                    message=wrapped_exc.message,
                    reason=wrapped_exc.reason,
                    context=wrapped_exc.context,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )

    async def _cheated_job_report_handler(self) -> None:
        """
        Listens for messages on the local cheated job reports channel and processes them.
        """
        while self.is_running():
            try:
                msg_or_none = await interruptible_receive_local_message(
                    CHEATED_JOB_REPORT_CHANNEL, stop_event=self._stop_event
                )
                if msg_or_none is not None:
                    cheated_job_report = V0JobCheated.model_validate(msg_or_none)
                    await process_miner_cheat_report(cheated_job_report)
            except asyncio.CancelledError:
                self._stop_event.set()
                VALIDATOR_FC_COMPONENT_STATE.labels(component=self.name).set(0)
                break
            except LocalChannelReceiveError as exc:
                self._logger.error(str(exc))
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_RECEIVE_ERROR,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except pydantic.ValidationError:
                msg = f"Invalid cheated job report received from facilitator: {msg_or_none}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                msg = f"Error handling cheated job report: {type(exc).__name__}: {exc}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )

    async def _send_job_rejected_message(
        self,
        job_uuid: str,
        message: str,
        rejected_by: JobParticipantType,
        reason: JobRejectionReason,
        context: FailureContext | None = None,
    ) -> None:
        msg = JobStatusUpdate(
            uuid=job_uuid,
            status=JobStatus.REJECTED,
            metadata=JobStatusMetadata(
                job_rejection_details=JobRejectionDetails(
                    rejected_by=rejected_by,
                    reason=reason,
                    message=message,
                    context=context,
                ),
            ),
        )
        await safe_send_local_message(channel=JOB_STATUS_UPDATE_CHANNEL, message=msg)

    async def _make_horde_failed_message(
        self,
        job_uuid: str,
        message: str,
        reported_by: JobParticipantType,
        reason: HordeFailureReason,
        context: FailureContext | None = None,
    ) -> None:
        msg = JobStatusUpdate(
            uuid=job_uuid,
            status=JobStatus.HORDE_FAILED,
            metadata=JobStatusMetadata(
                horde_failure_details=HordeFailureDetails(
                    reported_by=reported_by,
                    reason=reason,
                    message=message,
                    context=context,
                ),
            ),
        )
        await safe_send_local_message(channel=JOB_STATUS_UPDATE_CHANNEL, message=msg)

    async def start(self) -> None:
        """Starts the main client."""
        if self.is_running():
            return

        await super().start()

        self._job_request_listener_task = asyncio.create_task(self._job_request_handler())
        self._cheated_job_report_listener_task = asyncio.create_task(
            self._cheated_job_report_handler()
        )

    async def stop(self) -> None:
        """Stops the main client."""
        if not self.is_running():
            return

        await super().stop()

        try:
            await stop_task_gracefully(self._job_request_listener_task)
            self._job_request_listener_task = None
        except Exception as exc:
            self._logger.error(
                "Error stopping job request listener task: %s: %s", type(exc).__name__, exc
            )

        try:
            await stop_task_gracefully(self._cheated_job_report_listener_task)
            self._cheated_job_report_listener_task = None
        except Exception as exc:
            self._logger.error(
                "Error stopping cheated job report listener task: %s: %s", type(exc).__name__, exc
            )
