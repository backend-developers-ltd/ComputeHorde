import pathlib
import zipfile

from django.conf import settings

from compute_horde_miner.miner.receipt_store.base import BaseReceiptStore, Receipt

ZIP_FILENAME = "receipts.zip"


class LocalReceiptStore(BaseReceiptStore):
    async def store(self, receipts: list[Receipt]):
        if not receipts:
            return

        root = pathlib.Path(settings.LOCAL_RECEIPTS_ROOT)
        root.mkdir(parents=True, exist_ok=True)
        filepath = root / ZIP_FILENAME

        with zipfile.ZipFile(filepath, mode='w', compression=zipfile.ZIP_LZMA) as zf:
            for receipt in receipts:
                zf.writestr(receipt.payload.job_uuid + '.json', receipt.json())

    async def get_url(self) -> str:
        return settings.LOCAL_RECEIPTS_URL + ZIP_FILENAME
