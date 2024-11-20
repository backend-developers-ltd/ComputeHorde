from collections.abc import AsyncIterator

from channels.generic.websocket import AsyncWebsocketConsumer


class ReceiptsServer(AsyncWebsocketConsumer):
    def connect(self):
        pass

    def receive(self, text_data=None, bytes_data=None):
        pass


async def receipt_stream() -> AsyncIterator:
    pass
