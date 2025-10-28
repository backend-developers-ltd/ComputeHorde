import asyncio

import sentry_sdk
from compute_horde.fv_protocol.validator_requests import V0Heartbeat

from compute_horde_validator.validator.models import SystemEvent

from .base import BaseComponent
from .constants import HEARTBEAT_CHANNEL
from .exceptions import LocalChannelSendError
from .metrics import VALIDATOR_FC_COMPONENT_STATE
from .util import (
    interruptible_wait,
    log_system_error_event,
    safe_send_local_message,
    stop_task_gracefully,
)


class HeartbeatManager(BaseComponent):
    """
    Periodically sends heartbeat messages to the Django default channel layer.
    """

    HEARTBEAT_INTERVAL = 60.0

    def __init__(self) -> None:
        super().__init__()
        self._heartbeat_loop_task: asyncio.Task[None] | None = None

    async def _heartbeat_loop(self) -> None:
        """
        Send a heartbeat message to the Django default channel layer in regular
        intervals.
        """
        while self.is_running():
            try:
                await interruptible_wait(
                    timeout=self.HEARTBEAT_INTERVAL, stop_event=self._stop_event
                )
                await safe_send_local_message(channel=HEARTBEAT_CHANNEL, message=V0Heartbeat())
            except asyncio.CancelledError:
                self._stop_event.set()
                VALIDATOR_FC_COMPONENT_STATE.labels(component=self.name).set(0)
                break
            except LocalChannelSendError as exc:
                self._logger.error(str(exc))
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_SEND_ERROR,
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                msg = f"Error sending heartbeat message: {type(exc).__name__}: {exc}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                )

    async def start(self) -> None:
        """Starts the heartbeat manager."""
        if self.is_running():
            return

        await super().start()

        self._heartbeat_loop_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Stops the heartbeat manager."""
        if not self.is_running():
            return

        await super().stop()

        try:
            await stop_task_gracefully(self._heartbeat_loop_task)
            self._heartbeat_loop_task = None
        except Exception as exc:
            self._logger.error(
                "Error stopping heartbeat loop task: %s: %s", type(exc).__name__, exc
            )
