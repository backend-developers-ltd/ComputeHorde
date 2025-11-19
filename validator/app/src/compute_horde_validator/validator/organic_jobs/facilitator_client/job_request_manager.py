import asyncio

import pydantic
import sentry_sdk
from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest, V0JobCheated
from compute_horde.fv_protocol.validator_requests import (
    JobRejectionDetails,
    JobStatusMetadata,
    JobStatusUpdate,
)
from compute_horde.protocol_consts import (
    JobParticipantType,
    JobRejectionReason,
    JobStatus,
)

from compute_horde_validator.validator.models import SystemEvent

from .base import BaseComponent
from .constants import (
    CHEATED_JOB_REPORT_CHANNEL,
    JOB_REQUEST_CHANNEL,
    JOB_STATUS_UPDATE_CHANNEL,
    WAIT_ON_ERROR_INTERVAL,
)
from .exceptions import LocalChannelReceiveError
from .jobs_task import (
    JobRequestVerificationFailed,
    process_miner_cheat_report,
    verify_and_submit_organic_job_request,
)
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
                    await verify_and_submit_organic_job_request(job_request=job_request)
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
                # MessageManager verifies incoming messages from facilitator, so this will only be
                # triggered if the message is mangled on the way from the message manager to the job
                # request manager.
                msg = f"Invalid job request received from message manager: {msg_or_none}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except JobRequestVerificationFailed as exc:
                self._logger.error(
                    f"Job request verification failed: {job_request.model_dump_json()}"
                )
                job_status_update = JobStatusUpdate(
                    uuid=job_request.uuid,
                    status=JobStatus.REJECTED,
                    metadata=JobStatusMetadata(
                        job_rejection_details=JobRejectionDetails(
                            rejected_by=JobParticipantType.VALIDATOR,
                            reason=JobRejectionReason.INVALID_SIGNATURE,
                            message=exc.message,
                        ),
                    ),
                )
                await safe_send_local_message(
                    channel=JOB_STATUS_UPDATE_CHANNEL, message=job_status_update
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                msg = f"Error handling job request: {type(exc).__name__}: {exc}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
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
