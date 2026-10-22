import abc
import functools
import logging
from collections.abc import Awaitable, Callable
from typing import Generic, ParamSpec, TypeAlias, TypeVar

from channels.generic.websocket import AsyncWebsocketConsumer
from compute_horde.protocol_messages import GenericError
from pydantic import BaseModel

logger = logging.getLogger(__name__)

Params = ParamSpec("Params")
TResult = TypeVar("TResult")
AsyncCallable: TypeAlias = Callable[Params, Awaitable[TResult]]


def log_errors_explicitly(f: AsyncCallable[Params, TResult]) -> AsyncCallable[Params, TResult]:
    @functools.wraps(f)
    async def wrapper(*args: Params.args, **kwargs: Params.kwargs) -> TResult:
        try:
            return await f(*args, **kwargs)
        except Exception as ex:
            logger.exception("")
            raise ex

    return wrapper


TReceived = TypeVar("TReceived", bound=BaseModel)


class BaseConsumer(AsyncWebsocketConsumer, Generic[TReceived], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def parse_message(self, raw_msg: str | bytes) -> TReceived:
        """
        Parses a raw message into a pydantic model; raises ValueError, such as
        pydantic.ValidationError or json.JSONDecodeError, when it is malformed.
        """

    @abc.abstractmethod
    async def handle(self, msg: TReceived) -> None: ...

    async def connect(self):
        await self.accept()

    @log_errors_explicitly
    async def receive(self, text_data=None, bytes_data=None) -> None:
        if text_data is None:
            await self.reject_malformed_message("expected a text frame")
            return
        try:
            msg = self.parse_message(text_data)
        except ValueError as ex:
            await self.reject_malformed_message(str(ex))
            return

        if isinstance(msg, GenericError):
            try:
                raise RuntimeError(f"Received error message: {msg.model_dump_json()}")
            except Exception:
                logger.exception("")
                return

        await self.handle(msg)

    async def reject_malformed_message(self, reason: str) -> None:
        logger.error(f"Malformed message: {reason}")
        await self.send(GenericError(details=f"Malformed message: {reason}").model_dump_json())
