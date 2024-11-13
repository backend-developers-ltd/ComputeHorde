from asgiref.sync import async_to_sync
from django.core.management import BaseCommand

from compute_horde_validator.validator.receipts_transfer import ReceiptsClient


@async_to_sync
async def _do_receive():
    async with ReceiptsClient.session("ws://localhost:8000/v0.1/receipts") as client:
        await client.all()


class Command(BaseCommand):
    def handle(self, *args, **options):
        _do_receive()
