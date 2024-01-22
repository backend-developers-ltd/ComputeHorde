import abc
import functools
import logging
from typing import TypeVar

from channels.generic.websocket import AsyncWebsocketConsumer
from compute_horde.base_requests import ValidationError
from compute_horde.em_protocol.executor_requests import BaseExecutorRequest
from compute_horde.mv_protocol.validator_requests import BaseValidatorRequest

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseExecutorRequest | BaseValidatorRequest)


def log_errors_explicitly(f):
    @functools.wraps(f)
    async def wrapper(*args, **kwargs):
        try:
            return await f(*args, **kwargs)
        except Exception as ex:
            logger.error('', exc_info=True)
            raise ex
    return wrapper


class BaseConsumer[T](AsyncWebsocketConsumer, abc.ABC):
    @abc.abstractmethod
    def accepted_request_type(self) -> type[T]:
        pass

    @abc.abstractmethod
    def incoming_generic_error_class(self):
        pass

    @abc.abstractmethod
    def outgoing_generic_error_class(self):
        pass

    @abc.abstractmethod
    async def handle(self, msg: T):
        ...

    async def connect(self):
        await self.accept()

    @log_errors_explicitly
    async def receive(self, text_data=None, bytes_data=None):
        try:
            msg: T = self.accepted_request_type().parse(text_data)
        except ValidationError as ex:
            logger.error(f'Malformed message: {str(ex)}')
            await self.send(
                self.outgoing_generic_error_class()(details=f'Malformed message: {str(ex)}').model_dump_json()
            )
            return

        if isinstance(msg, self.incoming_generic_error_class()):
            try:
                raise RuntimeError(f'Received error message: {msg.model_dump_json()}')
            except Exception:
                logger.error('', exc_info=True)
                return

        await self.handle(msg)
