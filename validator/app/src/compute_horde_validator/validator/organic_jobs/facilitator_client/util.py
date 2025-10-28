from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, TypeVar, overload

from channels.layers import get_channel_layer
from django.conf import settings
from pydantic import BaseModel

from compute_horde_validator.validator.models import SystemEvent

from .constants import GRACEFULLY_STOP_TIMEOUT
from .exceptions import LocalChannelReceiveError, LocalChannelSendError, TransportLayerReceiveError

if TYPE_CHECKING:
    from .facilitator_connector import ConnectionManager

default_logger = logging.getLogger(__name__)


T = TypeVar("T")


class _GenericMessageReceiveError(Exception):
    """
    Placeholder exception to make it easier to isolate asyncio errors from
    message receive errors. Should never be expected anywhere.
    """

    def __init__(self, cause: Exception) -> None:
        self.cause = cause
        super().__init__(cause)


async def cancel_and_await_task(task: asyncio.Task[Any]) -> None:
    """
    A helper function that cancels a task and awaits it.
    """
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def stop_task_gracefully(
    task: asyncio.Task[Any] | None, timeout: float = GRACEFULLY_STOP_TIMEOUT
) -> None:
    """
    Waits for a task to end gracefully, cancels it if it doesn't end within
    the given timeout. Should typically be run after task shut down has been
    triggered elsewhere.

    Args:
        task (asyncio.Task | None): The task to wait for. If None, this
            function does nothing. Defaults to None.
        timeout (float): The timeout in seconds. Defaults to GRACEFULLY_STOP_TIMEOUT seconds.
    """
    if task and not task.done():
        try:
            await asyncio.wait_for(task, timeout=timeout)
        except TimeoutError:
            await cancel_and_await_task(task)


async def interruptible_wait(timeout: float = 1.0, stop_event: asyncio.Event | None = None) -> None:
    """
    Waits for a given amount of time with the option of interrupting the wait
    by a stop event being set.

    Args:
        timeout (float): The timeout in seconds. Defaults to 1.0 seconds.
        stop_event (asyncio.Event | None): The stop event to wait for. If not
            None, then the wait will be interrupted when the stop event is set.
            If None, the wait will not be interrupted. Defaults to None.
    """
    if stop_event is None:
        await asyncio.sleep(timeout)
        return
    if stop_event.is_set():  # Shortcut if the event is already set
        return

    sleep_task = asyncio.create_task(asyncio.sleep(timeout))
    interrupt_task = asyncio.create_task(stop_event.wait())
    await asyncio.wait(
        [sleep_task, interrupt_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    # Cancel the tasks to avoid task leaks
    await cancel_and_await_task(sleep_task)
    await cancel_and_await_task(interrupt_task)


async def safe_send_local_message(channel: str, message: BaseModel, timeout: float = 10.0) -> None:
    """
    Sends a message via the default Django channel layer and includes a timeout
    to ensure that functions that send messages don't hang indefinitely.

    Args:
        channel (str): The channel over which to send the message.
        message (BaseModel): The message to send.
        timeout (float): The timeout in seconds. Defaults to 10.0 seconds.

    Raises:
        LocalChannelSendError: If an error occurs while sending the message.
    """
    try:
        send_task = asyncio.create_task(
            get_channel_layer().send(channel, message.model_dump(mode="json"))
        )
        await asyncio.wait_for(send_task, timeout=timeout)
    except Exception as exc:
        await cancel_and_await_task(send_task)
        raise LocalChannelSendError(cause=exc, channel=channel)


async def log_system_error_event(
    message: str,
    event_type: SystemEvent.EventType,
    event_subtype: SystemEvent.EventSubType,
) -> None:
    """
    Logs a system error event to the database.

    Args:
        message (str): The message to log and save.
        event_type (SystemEvent.EventType): The type of the system event.
        event_subtype (SystemEvent.EventSubType): The subtype of the system event.
    """
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=event_type,
        subtype=event_subtype,
        long_description=message,
        data={},
    )


@overload
async def _interruptible_receive_message_helper(
    awaitable_coroutine: Callable[[], Coroutine[Any, Any, str | bytes]],
    stop_event: asyncio.Event | None = None,
) -> str | bytes | None: ...


@overload
async def _interruptible_receive_message_helper(
    awaitable_coroutine: Callable[[], Coroutine[Any, Any, dict[str, Any]]],
    stop_event: asyncio.Event | None = None,
) -> dict[str, Any] | None: ...


async def _interruptible_receive_message_helper(
    awaitable_coroutine: Callable[[], Coroutine[Any, Any, str | bytes | dict[str, Any]]],
    stop_event: asyncio.Event | None = None,
) -> str | bytes | dict[str, Any] | None:
    """
    Helper function that contains common code for interruptible_receive_local_message
    and interruptible_receive_transport_layer_message. Should not be used on its own.

    Raises:
        _GenericMessageReceiveError: If an error occurs while receiving the message.
    """
    if stop_event is None:
        return await awaitable_coroutine()
    if stop_event.is_set():  # Shortcut if the event is already set
        return None

    receive_task = asyncio.create_task(awaitable_coroutine())
    interrupt_task = asyncio.create_task(stop_event.wait())
    await asyncio.wait(
        [receive_task, interrupt_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    # Cancel potentially unfinished task to prevent task leaks
    if receive_task.done():
        await cancel_and_await_task(interrupt_task)
        try:
            return await receive_task
        except Exception as exc:
            raise _GenericMessageReceiveError(exc)
    else:
        await cancel_and_await_task(receive_task)
        await cancel_and_await_task(interrupt_task)
        return None


async def interruptible_receive_local_message(
    channel: str, stop_event: asyncio.Event | None = None
) -> dict[str, Any] | None:
    """
    Waits for a message on a specific local Django channel with the option of
    cancelling a blocking receive call by a stop event.

    Args:
        channel (str): The channel to receive the message from.
        stop_event (asyncio.Event | None): The stop event to wait for. If not
            None, then the receive will be interrupted when the stop event is set.
            If None, the receive will not be interrupted. Defaults to None.

    Raises:
        LocalChannelReceiveError: If an error occurs while receiving the message.

    Returns:
        dict | None: The message received from the channel or None if the receive
            was interrupted.
    """
    try:
        return await _interruptible_receive_message_helper(  # type: ignore
            lambda: get_channel_layer().receive(channel), stop_event
        )
    except _GenericMessageReceiveError as exc:
        raise LocalChannelReceiveError(cause=exc.cause, channel=channel)


async def interruptible_receive_transport_layer_message(
    connection_manager: ConnectionManager, stop_event: asyncio.Event | None = None
) -> str | bytes | None:
    """
    Waits for a message from the transport layer via the connection manager with the option of
    canceling a blocking receive call by a stop event.

    Args:
        connection_manager (ConnectionManager): The connection manager over which
            to receive the message.
        stop_event (asyncio.Event | None): The stop event to wait for. If not
            None, then the receive will be interrupted when the stop event is set.
            If None, the receive will not be interrupted. Defaults to None.

    Raises:
        TransportLayerReceiveError: If an error occurs while receiving the message.

    Returns:
        str | None: The message received from the transport layer or None if the receive
            was interrupted.
    """
    try:
        return await _interruptible_receive_message_helper(
            awaitable_coroutine=connection_manager.receive, stop_event=stop_event
        )
    except _GenericMessageReceiveError as exc:
        raise TransportLayerReceiveError(exc.cause)
