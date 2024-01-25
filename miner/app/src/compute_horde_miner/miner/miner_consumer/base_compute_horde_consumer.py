import abc
import functools
import logging

from channels.generic.websocket import AsyncWebsocketConsumer
from compute_horde.base_requests import BaseRequest, ValidationError

logger = logging.getLogger(__name__)


def log_errors_explicitly(f):
    @functools.wraps(f)
    async def wrapper(*args, **kwargs):
        try:
            return await f(*args, **kwargs)
        except Exception as ex:
            logger.exception('')
            raise ex
    return wrapper


class BaseConsumer(AsyncWebsocketConsumer, abc.ABC):
    @abc.abstractmethod
    def accepted_request_type(self) -> type[BaseRequest]:
        pass

    @abc.abstractmethod
    def incoming_generic_error_class(self):
        pass

    @abc.abstractmethod
    def outgoing_generic_error_class(self):
        pass

    @abc.abstractmethod
    async def handle(self, msg):
        ...

    async def connect(self):
        await self.accept()

    @log_errors_explicitly
    async def receive(self, text_data=None, bytes_data=None):
        try:
            msg = self.accepted_request_type().parse(text_data)
        except ValidationError as ex:
            logger.error(f'Malformed message: {str(ex)}')
            await self.send(
                self.outgoing_generic_error_class()(details=f'Malformed message: {str(ex)}').json()
            )
            return

        if isinstance(msg, self.incoming_generic_error_class()):
            try:
                raise RuntimeError(f'Received error message: {msg.json()}')
            except Exception:
                logger.exception('')
                return

        await self.handle(msg)
