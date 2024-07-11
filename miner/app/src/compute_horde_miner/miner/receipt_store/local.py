import csv
import io
import pathlib
import shutil
import tempfile

from compute_horde.mv_protocol.validator_requests import (
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.receipts import Receipt, ReceiptType
from django.conf import settings

from compute_horde_miner.miner.receipt_store.base import BaseReceiptStore

FILENAME = "receipts.csv"


class LocalReceiptStore(BaseReceiptStore):
    def store(self, receipts: list[Receipt]) -> None:
        if not receipts:
            return

        payload_fields = set()
        for payload_cls in [JobStartedReceiptPayload, JobFinishedReceiptPayload]:
            payload_fields |= set(payload_cls.model_fields.keys())

        buf = io.StringIO()
        csv_writer = csv.DictWriter(
            buf,
            [
                "type",
                "validator_signature",
                "miner_signature",
                *payload_fields,
            ],
        )
        csv_writer.writeheader()
        for receipt in receipts:
            match receipt.payload:
                case JobStartedReceiptPayload():
                    receipt_type = ReceiptType.JobStartedReceipt
                case JobFinishedReceiptPayload():
                    receipt_type = ReceiptType.JobFinishedReceipt
            row = (
                dict(
                    type=receipt_type.value,
                    validator_signature=receipt.validator_signature,
                    miner_signature=receipt.miner_signature,
                )
                | receipt.payload.model_dump()
            )
            csv_writer.writerow(row)

        root = pathlib.Path(settings.LOCAL_RECEIPTS_ROOT)
        root.mkdir(parents=True, exist_ok=True)
        filepath = root / FILENAME

        with tempfile.NamedTemporaryFile(mode="wt", delete=False, encoding="utf8") as temp_file:
            temp_file.write(buf.getvalue())

        shutil.move(temp_file.name, filepath)
        filepath.chmod(0o644)
