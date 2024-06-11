import csv
import pathlib
import shutil
import tempfile

from compute_horde.receipts import Receipt
from django.conf import settings

from compute_horde_miner.miner.receipt_store.base import BaseReceiptStore

FILENAME = "receipts.csv"


class LocalReceiptStore(BaseReceiptStore):
    def store(self, receipts: list[Receipt]) -> None:
        if not receipts:
            return

        root = pathlib.Path(settings.LOCAL_RECEIPTS_ROOT)
        root.mkdir(parents=True, exist_ok=True)
        filepath = root / FILENAME

        with tempfile.NamedTemporaryFile(mode="w", delete=False, newline="") as temp_file:
            csv_writer = csv.writer(temp_file)
            csv_writer.writerow(
                [
                    "job_uuid",
                    "miner_hotkey",
                    "validator_hotkey",
                    "time_started",
                    "time_took_us",
                    "score_str",
                    "validator_signature",
                    "miner_signature",
                ]
            )

            for receipt in receipts:
                csv_writer.writerow(
                    [
                        receipt.payload.job_uuid,
                        receipt.payload.miner_hotkey,
                        receipt.payload.validator_hotkey,
                        receipt.payload.time_started.isoformat(),
                        receipt.payload.time_took_us,
                        receipt.payload.score_str,
                        receipt.validator_signature,
                        receipt.miner_signature,
                    ]
                )

        shutil.move(temp_file.name, filepath)
        filepath.chmod(0o644)

    def get_url(self) -> str:
        return settings.LOCAL_RECEIPTS_URL + FILENAME
