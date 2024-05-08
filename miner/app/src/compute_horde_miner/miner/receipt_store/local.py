import datetime
import pathlib
import zipfile

from django.conf import settings

from compute_horde_miner.miner.receipt_store.base import BaseReceiptStore, Receipt


class LocalReceiptStore(BaseReceiptStore):
    async def store(self, date: datetime.date, receipts: list[Receipt]):
        if not receipts:
            return

        root = pathlib.Path(settings.LOCAL_RECEIPTS_ROOT)
        root.mkdir(parents=True, exist_ok=True)
        filepath = root / self._get_file_name(date)

        with zipfile.ZipFile(filepath, mode='w', compression=zipfile.ZIP_LZMA) as zf:
            for receipt in receipts:
                zf.writestr(receipt.payload.job_uuid + '.json', receipt.json())

    async def get_url(self, date: datetime.date) -> str:
        return settings.LOCAL_RECEIPTS_URL + self._get_file_name(date)

    def _get_file_name(self, date: datetime.date) -> str:
        return date.isoformat() + ".zip"
