import logging

from channels.generic.websocket import AsyncWebsocketConsumer

logger = logging.getLogger(__name__)


class MinerExecutorConsumer(AsyncWebsocketConsumer):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)

    async def connect(self):
        await self.accept()

    async def receive(self, text_data=None, bytes_data=None):
        await self.send(text_data="Hello world!")

    async def disconnect(self, close_code):
        logger.info('Executor disconnected')
