import asyncio
import os
import time
from collections import deque
from typing import Any

import bittensor_wallet
import httpx
import pydantic
import sentry_sdk
from compute_horde.fv_protocol.facilitator_requests import (
    Error,
    OrganicJobRequest,
    Response,
    V0JobCheated,
)
from compute_horde.fv_protocol.validator_requests import (
    JobStatusUpdate,
    V0AuthenticationRequest,
    V0Heartbeat,
)
from compute_horde.transport import AbstractTransport, TransportConnectionError
from django.conf import settings
from pydantic import BaseModel

from compute_horde_validator.validator.models import SystemEvent

from .base import BaseComponent
from .constants import (
    CHEATED_JOB_REPORT_CHANNEL,
    HEARTBEAT_CHANNEL,
    JOB_REQUEST_CHANNEL,
    JOB_STATUS_UPDATE_CHANNEL,
    TRANSPORT_LAYER_POLL_INTERVAL,
    WAIT_ON_ERROR_INTERVAL,
)
from .exceptions import LocalChannelSendError
from .metrics import (
    VALIDATOR_FC_COMPONENT_STATE,
    VALIDATOR_FC_MESSAGE_QUEUE_LENGTH,
    VALIDATOR_FC_MESSAGE_SEND_DURATION,
    VALIDATOR_FC_MESSAGE_SEND_FAILURES,
    VALIDATOR_FC_MESSAGES_RECEIVED,
    VALIDATOR_FC_MESSAGES_SENT,
    VALIDATOR_FC_TRANSPORT_LAYER_AUTHENTICATION_DURATION,
    VALIDATOR_FC_TRANSPORT_LAYER_CONNECTION_DURATION,
    VALIDATOR_FC_TRANSPORT_LAYER_EVENTS,
    VALIDATOR_FC_TRANSPORT_LAYER_STATE,
    timing_decorator,
)
from .util import (
    cancel_and_await_task,
    interruptible_receive_local_message,
    interruptible_receive_transport_layer_message,
    interruptible_wait,
    log_system_error_event,
    safe_send_local_message,
    stop_task_gracefully,
)


class AuthenticationError(Exception):
    def __init__(self, reason: str, errors: list[Error]) -> None:
        self.reason = reason
        self.errors = errors


class ConnectionManager(BaseComponent):
    """
    Periodically checks that the connection across a transport layer is still
    active and reconnects if it isn't.
    """

    TL_TIMEOUT = 10.0
    AUTH_SEND_TIMEOUT = 10.0
    AUTH_RECEIVE_TIMEOUT = 10.0
    WEBHOOK_TIMEOUT = 10.0
    ADDITIONAL_HTTP_HEADERS = {
        "X-Validator-Runner-Version": os.environ.get("VALIDATOR_RUNNER_VERSION", "unknown"),
        "X-Validator-Version": os.environ.get("VALIDATOR_VERSION", "unknown"),
    }

    def __init__(
        self,
        keypair: bittensor_wallet.Keypair,
        transport_layer: AbstractTransport,
    ) -> None:
        """
        Args:
            keypair (bittensor_wallet.Keypair): The keypair to use for authentication.
            transport_layer (AbstractTransport): The transport layer to manage
                the connection for. It is expected that this transport layer
                handles its own reconnection logic as it may be specific to the
                connection type.
        """
        super().__init__()
        self.transport_layer = transport_layer
        self.keypair = keypair
        self._authentication_flag = asyncio.Event()
        self._cleanup_event = asyncio.Event()
        self._main_task: asyncio.Task[None] | None = None
        self._http_client: httpx.AsyncClient | None = None

    @timing_decorator(VALIDATOR_FC_TRANSPORT_LAYER_CONNECTION_DURATION)
    async def _connect_transport_layer_with_timeout(self) -> None:
        """Connects to the transport layer with a timeout."""
        try:
            await asyncio.wait_for(
                self.transport_layer.start(additional_headers=self.ADDITIONAL_HTTP_HEADERS),
                timeout=self.TL_TIMEOUT,
            )
        except TimeoutError:
            raise TransportConnectionError("transport layer connection timed out")

    @timing_decorator(VALIDATOR_FC_TRANSPORT_LAYER_AUTHENTICATION_DURATION)
    async def _authenticate_connection(self) -> None:
        """Authenticates the connection with the facilitator."""
        # Set to False to ensure authentication doesn't become stale
        self._authentication_flag.clear()

        if not self.transport_layer.is_connected():
            raise AuthenticationError(
                "Transport layer must be connected before authentication is possible", []
            )

        try:
            await asyncio.wait_for(
                self.transport_layer.send(
                    V0AuthenticationRequest.from_keypair(self.keypair).model_dump_json()
                ),
                timeout=self.AUTH_SEND_TIMEOUT,
            )
        except TimeoutError:
            raise AuthenticationError("authentication send timed out", [])

        try:
            raw_msg = await asyncio.wait_for(
                self.transport_layer.receive(),
                timeout=self.AUTH_RECEIVE_TIMEOUT,
            )
        except TimeoutError:
            raise AuthenticationError("authentication receive response timed out", [])

        try:
            response = Response.model_validate_json(raw_msg)
        except pydantic.ValidationError as exc:
            raise AuthenticationError(
                f"did not receive Response for V0AuthenticationRequest. Got ({raw_msg!r}) instead",
                [],
            ) from exc
        if response.status != "success":
            raise AuthenticationError("auth request received failed response", response.errors)

        self._authentication_flag.set()

    async def _call_debug_connect_facilitator_webhook(self) -> None:
        if settings.DEBUG_CONNECT_FACILITATOR_WEBHOOK:
            if self._http_client is None:
                self._http_client = httpx.AsyncClient()
            try:
                await self._http_client.get(
                    settings.DEBUG_CONNECT_FACILITATOR_WEBHOOK, timeout=self.WEBHOOK_TIMEOUT
                )
            except Exception:
                self._logger.info("when calling connect webhook:", exc_info=True)

    async def _establish_connection(self) -> None:
        """Connect and authenticate the transport layer and call an optional debug webhook."""
        await self._connect_transport_layer_with_timeout()
        await self._authenticate_connection()
        await self._call_debug_connect_facilitator_webhook()

    async def _cleanup_resources(self) -> None:
        """
        Clean up resources.
        """
        if self._cleanup_event.is_set():
            return

        self._cleanup_event.set()

        if self._http_client is not None:
            try:
                close_task = asyncio.create_task(self._http_client.aclose())
                await asyncio.wait_for(close_task, timeout=1.0)
            except TimeoutError:
                await cancel_and_await_task(close_task)
            except Exception as exc:
                self._logger.error("Error closing HTTP client: %s: %s", type(exc).__name__, exc)
            finally:
                self._http_client = None

        try:
            # Long timeout to allow anything still being transferred to complete
            stop_task = asyncio.create_task(self.transport_layer.stop())
            await asyncio.wait_for(stop_task, timeout=10.0)
            VALIDATOR_FC_TRANSPORT_LAYER_STATE.set(0)
        except TimeoutError:
            await cancel_and_await_task(stop_task)
        except Exception as exc:
            self._logger.error(
                "Error disconnecting transport layer: %s: %s", type(exc).__name__, exc
            )

    async def _monitor_connection(self) -> None:
        """
        Check if the transport layer is connected and try to reconnect if it isn't
        """
        while self.is_running():
            try:
                if not self.transport_layer.is_connected():
                    VALIDATOR_FC_TRANSPORT_LAYER_STATE.set(0)
                    await self._establish_connection()
                    VALIDATOR_FC_TRANSPORT_LAYER_STATE.set(1)
                    VALIDATOR_FC_TRANSPORT_LAYER_EVENTS.labels(event="success").inc()
                # Reduce polling of transport layer
                await interruptible_wait(
                    timeout=TRANSPORT_LAYER_POLL_INTERVAL, stop_event=self._stop_event
                )
            except asyncio.CancelledError:
                self._stop_event.set()
                await self._cleanup_resources()
                VALIDATOR_FC_COMPONENT_STATE.labels(component=self.name).set(0)
                break
            except TransportConnectionError as exc:
                msg = f"Transport layer connection error: {type(exc).__name__}: {exc}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.TRANSPORT_CONNECTION_ERROR,
                )
                VALIDATOR_FC_TRANSPORT_LAYER_EVENTS.labels(event="transport_error").inc()
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except AuthenticationError as exc:
                msg = f"Authentication error: {type(exc).__name__}: {exc}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.AUTHENTICATION_ERROR,
                )
                VALIDATOR_FC_TRANSPORT_LAYER_EVENTS.labels(event="auth_error").inc()
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                msg = f"Unexpected error: {type(exc).__name__}: {exc}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                )
                VALIDATOR_FC_TRANSPORT_LAYER_EVENTS.labels(event="unknown_error").inc()
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )

    def is_connected_and_authenticated(self) -> bool:
        """Checks if the connection is connected and authenticated."""
        return self.transport_layer.is_connected() and self._authentication_flag.is_set()

    async def receive(self) -> str | bytes:
        """Receives a message from the transport layer."""
        return await self.transport_layer.receive()

    async def send(self, message: str) -> None:
        """Sends a message to the transport layer."""
        await self.transport_layer.send(message)

    async def start(self) -> None:
        """Starts the connection manager main loop."""
        if self.is_running():
            return

        await super().start()

        self._cleanup_event.clear()
        self._authentication_flag.clear()
        self._main_task = asyncio.create_task(self._monitor_connection())

    async def stop(self) -> None:
        """
        Stop the connection manager main loop and clean up resources.
        """
        if not self.is_running():
            return

        await super().stop()

        try:
            # Long timeout to allow the transport layer to finish transmitting
            # any messages and close gracefully
            await stop_task_gracefully(task=self._main_task, timeout=15.0)
            self._main_task = None
        except Exception as exc:
            self._logger.error(
                "Error in connection manager main loop: %s: %s", type(exc).__name__, exc
            )

        # Attempt to run cleanup again in case the monitor loop couldn't be stopped gracefully
        await self._cleanup_resources()

        self._authentication_flag.clear()


class MessageWrapper(BaseModel):
    """A simple wrapper around a message that allows for retry counting"""

    content: BaseModel
    retry_count: int = 0
    max_retries: int = 3


class MessageTypeException(Exception):
    def __init__(self, message: str | bytes) -> None:
        super().__init__(f"Unknown message type: {message!r}")


class MessageChannelException(Exception):
    def __init__(self, message: str | bytes, channel: str) -> None:
        super().__init__(f"Got a valid message ({message!r}) on an unexpected channel ({channel})")


class MessageRetryLimitExceeded(Exception):
    def __init__(self, message: MessageWrapper) -> None:
        super().__init__(
            f"Failed to send message ({message.content}) after {message.retry_count} retries"
        )


class MessageManager(BaseComponent):
    """
    Handles messaging between the facilitator and the validator components.

    Performs the following operations:
    - Manages queued messages to be sent to the facilitator via the transport
      layer
    - Listens for messages from the facilitator and pushes them into the
      default Django channel layer
    - Subscribes to messages from the default Django channel layer and pushes
      them into the message queue to be sent to the facilitator.
    """

    MAX_MESSAGE_SEND_RETRIES = 5
    MSG_RETRY_DELAY = 5.0
    EMPTY_MSG_QUEUE_BACKOFF_INTERVAL = 1.0
    TRANSPORT_LAYER_MESSAGE_SEND_TIMEOUT = 10.0

    def __init__(self, connection_manager: ConnectionManager) -> None:
        """
        Args:
            connection_manager (ConnectionManager): The connection manager to use
                to interact with the transport layer.
        """
        super().__init__()
        self.connection_manager = connection_manager
        self._queue: deque[MessageWrapper] = deque()
        self._queue_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()

        self._transport_layer_listener_task: asyncio.Task[None] | None = None
        self._message_sender_task: asyncio.Task[None] | None = None
        self._heartbeat_listener_task: asyncio.Task[None] | None = None
        self._job_status_update_listener_task: asyncio.Task[None] | None = None

    async def _enqueue_message(self, message: BaseModel) -> None:
        """
        Adds a message to the end of the queue
        """
        wrapped_msg = MessageWrapper(content=message, max_retries=self.MAX_MESSAGE_SEND_RETRIES)
        async with self._queue_lock:
            self._queue.append(wrapped_msg)
            VALIDATOR_FC_MESSAGE_QUEUE_LENGTH.set(len(self._queue))

    async def _get_next_message(self) -> MessageWrapper | None:
        """
        Gets the oldest message in the queue (FIFO principle).

        Returns:
            MessageWrapper | None: Either a queued message or None if the queue
                is empty.

        """
        async with self._queue_lock:
            try:
                msg = self._queue.popleft()
                VALIDATOR_FC_MESSAGE_QUEUE_LENGTH.set(len(self._queue))
                return msg
            except IndexError:  # Empty queue
                VALIDATOR_FC_MESSAGE_QUEUE_LENGTH.set(0)
                return None

    async def _retry_message(self, message: MessageWrapper) -> None:
        """
        Inserts message into the front of the queue again to retry sending it.

        Raises:
            MessageRetryLimitExceeded: If the message has reached the maximum number of retries.
        """
        async with self._queue_lock:
            if message.retry_count < message.max_retries:
                message.retry_count += 1
                self._queue.appendleft(message)
                VALIDATOR_FC_MESSAGE_QUEUE_LENGTH.set(len(self._queue))
            else:
                VALIDATOR_FC_MESSAGE_SEND_FAILURES.labels(
                    message_type=type(message.content).__name__
                ).inc()
                raise MessageRetryLimitExceeded(message)

    async def _get_queue_length(self) -> int:
        """Get the length of the queue while avoiding race conditions."""
        async with self._queue_lock:
            return len(self._queue)

    async def _process_incoming_transport_layer_message(self, message: str | bytes) -> BaseModel:
        """
        Parses an incoming message from the transport layer and takes the
        appropriate action.

        Expects one of the following message types:
            - Response (sent by the facilitator to acknowledge messages)
            - OrganicJobRequest
            - V0JobCheated

        Args:
            message (str): The message to parse.

        Raises:
            LocalChannelSendError: If an error occurs while sending the message to the local channel.
            MessageTypeException: If the message type is unknown.

        Returns:
            BaseModel: The parsed message.
        """
        try:
            response = Response.model_validate_json(message)
        except pydantic.ValidationError:
            pass
        else:
            if response.status != "success":
                self._logger.error(
                    "received error response from facilitator: %r", response.model_dump_json()
                )
            return response

        try:
            job_request = pydantic.TypeAdapter(OrganicJobRequest).validate_json(message)
        except pydantic.ValidationError:
            pass
        else:
            await safe_send_local_message(
                channel=JOB_REQUEST_CHANNEL,
                message=job_request,
            )
            return job_request

        try:
            cheated_job_report = pydantic.TypeAdapter(V0JobCheated).validate_json(message)
        except pydantic.ValidationError:
            pass
        else:
            await safe_send_local_message(
                channel=CHEATED_JOB_REPORT_CHANNEL,
                message=cheated_job_report,
            )
            return cheated_job_report

        raise MessageTypeException(message)

    async def _listen_for_transport_layer_messages(self) -> None:
        """
        Listens for messages from the transport layer and adds them to the
        appropriate Django channel.
        """
        while self.is_running():
            try:
                # If transport layer isn't connected and authenticated, wait
                # (and hope) for ConnectionManager to (re-)establish the connection.
                # This also ensures that the message manager doesn't accidentally
                # grab the authentication message
                if not self.connection_manager.is_connected_and_authenticated():
                    await interruptible_wait(
                        timeout=TRANSPORT_LAYER_POLL_INTERVAL, stop_event=self._stop_event
                    )
                    continue

                message = await interruptible_receive_transport_layer_message(
                    connection_manager=self.connection_manager,
                    stop_event=self._stop_event,
                )
                if message is not None:
                    validated_message = await self._process_incoming_transport_layer_message(
                        message
                    )
                    VALIDATOR_FC_MESSAGES_RECEIVED.labels(
                        message_type=type(validated_message).__name__
                    ).inc()

                # The transport_layer.receive method is blocking for the
                # websockets transport layer but this might not be the case for
                # other transport layers. To avoid excessive polling of the
                # transport layer, an additional cool-down wait is included here.
                await interruptible_wait(
                    timeout=TRANSPORT_LAYER_POLL_INTERVAL, stop_event=self._stop_event
                )
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
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except MessageTypeException as exc:
                self._logger.error(str(exc))
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                )
                VALIDATOR_FC_MESSAGES_RECEIVED.labels(message_type="unknown").inc()
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                msg = f"Error listening to incoming transport layer messages: {type(exc).__name__}: {exc}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )

    async def _try_to_send_next_message(self) -> None:
        """
        Attempt to send the next message in the queue with retries.

        Raises:
            MessageRetryLimitExceeded: If the message has reached the maximum number of retries.
        """
        async with self._send_lock:
            msg = await self._get_next_message()
            if msg is None:
                return
            try:
                send_task = None
                start = time.monotonic()
                send_task = asyncio.create_task(
                    self.connection_manager.send(msg.content.model_dump_json())
                )
                await asyncio.wait_for(send_task, timeout=self.TRANSPORT_LAYER_MESSAGE_SEND_TIMEOUT)
            except Exception as exc:
                if send_task is None:
                    # Asyncio could fail in creating the send task itself which would be no fault of the connection itself and shouldn't count against the message retries
                    self._logger.error(
                        "Failed to create send task for message (%s) with error (%s: %s) and attempting to retry",
                        msg.content,
                        type(exc).__name__,
                        exc,
                    )
                    # Subtract so that when _retry_message increments, it's net 0
                    msg.retry_count -= 1
                else:
                    await cancel_and_await_task(send_task)

                await self._retry_message(msg)  # Raises the MessageRetryLimitExceeded exception

                self._logger.debug(
                    "Failed to send message (%s) with error (%s: %s) and attempting to retry",
                    msg.content,
                    type(exc).__name__,
                    exc,
                )

                # Brief wait to allow whatever problem prevented the message to be sent
                # to (hopefully) be fixed elsewhere
                await interruptible_wait(timeout=self.MSG_RETRY_DELAY, stop_event=self._stop_event)
            else:
                VALIDATOR_FC_MESSAGE_SEND_DURATION.labels(
                    message_type=type(msg.content).__name__
                ).observe(time.monotonic() - start)
                VALIDATOR_FC_MESSAGES_SENT.labels(
                    message_type=type(msg.content).__name__,
                    retries=msg.retry_count,
                ).inc()

    async def _send_remaining_messages(self) -> None:
        """Attempt to send all remaining messages in the queue."""
        while True:
            if await self._get_queue_length() == 0:
                break
            try:
                await self._try_to_send_next_message()
            except MessageRetryLimitExceeded as exc:
                # This is a cleanup function -> errors can only be logged and accepted at this point
                self._logger.error(str(exc))

    async def _send_messages(self) -> None:
        """
        Goes through message stored in the queue and attempts to send them
        through the transport layer.
        """
        while self.is_running():
            try:
                if await self._get_queue_length() == 0:
                    await interruptible_wait(
                        timeout=self.EMPTY_MSG_QUEUE_BACKOFF_INTERVAL, stop_event=self._stop_event
                    )
                    continue

                # If transport layer isn't connected, wait (and hope) for
                # ConnectionManager to re-establish the connection.
                if not self.connection_manager.is_connected_and_authenticated():
                    await interruptible_wait(
                        timeout=TRANSPORT_LAYER_POLL_INTERVAL, stop_event=self._stop_event
                    )
                    continue

                await self._try_to_send_next_message()
            except asyncio.CancelledError:
                self._stop_event.set()
                await self._send_remaining_messages()
                break
            except MessageRetryLimitExceeded as exc:
                self._logger.error(str(exc))
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_SEND_ERROR,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                msg = f"Error sending messages: {type(exc).__name__}: {exc}"
                self._logger.error(msg)
                await log_system_error_event(
                    message=msg,
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )

    async def _process_incoming_local_message(self, msg: dict[str, Any], channel: str) -> None:
        """
        Validates a message from the default Django channel and adds it to the
        message queue.

        Args:
            msg (dict): The message to validate.

        Raises:
            MessageTypeException: If the message type is unknown.
        """
        try:
            job_status_update: JobStatusUpdate = JobStatusUpdate.model_validate(msg)
        except pydantic.ValidationError:
            pass
        else:
            if channel != JOB_STATUS_UPDATE_CHANNEL:
                raise MessageChannelException(str(msg), channel)
            await self._enqueue_message(job_status_update)
            return

        try:
            heartbeat: V0Heartbeat = V0Heartbeat.model_validate(msg)
        except pydantic.ValidationError:
            pass
        else:
            if channel != HEARTBEAT_CHANNEL:
                raise MessageChannelException(str(msg), channel)
            await self._enqueue_message(heartbeat)
            return

        raise MessageTypeException(str(msg))

    async def _listen_for_local_messages(self, channel: str) -> None:
        """
        Listen for messages on the default Django channel and place them into the message queue.
        """
        while self.is_running():
            try:
                msg_or_none = await interruptible_receive_local_message(
                    channel, stop_event=self._stop_event
                )
                if msg_or_none is not None:
                    await self._process_incoming_local_message(msg=msg_or_none, channel=channel)
            except asyncio.CancelledError:
                self._stop_event.set()
                break
            except (MessageTypeException, MessageChannelException) as exc:
                self._logger.error(str(exc))
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                msg = f"Error listening for local messages: {type(exc).__name__}: {exc}"
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
        """Starts the message manager."""
        if self.is_running():
            return

        await super().start()

        self._transport_layer_listener_task = asyncio.create_task(
            self._listen_for_transport_layer_messages()
        )
        self._heartbeat_listener_task = asyncio.create_task(
            self._listen_for_local_messages(HEARTBEAT_CHANNEL)
        )
        self._job_status_update_listener_task = asyncio.create_task(
            self._listen_for_local_messages(JOB_STATUS_UPDATE_CHANNEL)
        )
        self._message_sender_task = asyncio.create_task(self._send_messages())

    async def stop(self) -> None:
        """
        Ends message manager. Attempts to send all remaining messages to clear
        the queue.
        """
        if not self.is_running():
            return

        await super().stop()

        # Stop listening for local messages first to prevent messages getting
        # stuck in the queue after the message sender task has been shut off
        # --> messages may now get stuck in the Redis queue
        try:
            await stop_task_gracefully(self._job_status_update_listener_task)
        except Exception as exc:
            self._logger.error(
                "Error stopping job status update listener task: %s: %s", type(exc).__name__, exc
            )

        try:
            await stop_task_gracefully(self._heartbeat_listener_task)
        except Exception as exc:
            self._logger.error(
                "Error stopping heartbeat listener task: %s: %s", type(exc).__name__, exc
            )

        try:
            await stop_task_gracefully(self._transport_layer_listener_task)
        except Exception as exc:
            self._logger.error(
                "Error stopping transport layer listener task: %s: %s", type(exc).__name__, exc
            )

        try:
            await stop_task_gracefully(self._message_sender_task)
        except Exception as exc:
            self._logger.error(
                "Error stopping message sender task: %s: %s", type(exc).__name__, exc
            )

        # Attempt to clear the queue one final time. This may have already happened
        # in the finally-block of the _send_messages method but if there are too many
        # messages this may have cancelled too soon.
        await self._send_remaining_messages()

        self._transport_layer_listener_task = None
        self._message_sender_task = None
        self._heartbeat_listener_task = None
        self._job_status_update_listener_task = None


class FacilitatorClient:
    """A helper class to manage the ConnectionManager and MessageManager in a single class."""

    def __init__(
        self, keypair: bittensor_wallet.Keypair, transport_layer: AbstractTransport
    ) -> None:
        self.connection_manager = ConnectionManager(keypair, transport_layer)
        self.message_manager = MessageManager(self.connection_manager)

    async def start(self) -> None:
        await self.connection_manager.start()
        await self.message_manager.start()

    async def stop(self) -> None:
        await self.message_manager.stop()
        await self.connection_manager.stop()
