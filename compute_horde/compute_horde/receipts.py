import contextlib
import csv
import datetime
import io
import logging
import shutil
import tempfile

import bittensor
import pydantic
import requests

from .mv_protocol.validator_requests import ReceiptPayload

logger = logging.getLogger(__name__)


class Receipt(pydantic.BaseModel):
    payload: ReceiptPayload
    validator_signature: str
    miner_signature: str

    def verify_miner_signature(self):
        miner_keypair = bittensor.Keypair(ss58_address=self.payload.miner_hotkey)
        return miner_keypair.verify(self.payload.blob_for_signing(), self.miner_signature)

    def verify_validator_signature(self):
        validator_keypair = bittensor.Keypair(ss58_address=self.payload.validator_hotkey)
        return validator_keypair.verify(self.payload.blob_for_signing(), self.validator_signature)


class ReceiptFetchError(Exception):
    pass


def get_miner_receipts(hotkey: str, ip: str, port: int) -> list[Receipt]:
    """Get receipts from a given miner"""
    with contextlib.ExitStack() as exit_stack:
        try:
            receipts_url = f"http://{ip}:{port}/receipts/receipts.csv"
            response = exit_stack.enter_context(requests.get(receipts_url, stream=True, timeout=5))
            response.raise_for_status()
        except requests.RequestException as e:
            raise ReceiptFetchError("failed to get receipts from miner") from e

        temp_file = exit_stack.enter_context(tempfile.TemporaryFile())
        shutil.copyfileobj(response.raw, temp_file)
        temp_file.seek(0)

        receipts = []
        wrapper = io.TextIOWrapper(temp_file)
        csv_reader = csv.DictReader(wrapper)
        for raw_receipt in csv_reader:
            try:
                receipt = Receipt(
                    payload=ReceiptPayload(
                        job_uuid=raw_receipt["job_uuid"],
                        miner_hotkey=raw_receipt["miner_hotkey"],
                        validator_hotkey=raw_receipt["validator_hotkey"],
                        time_started=datetime.datetime.fromisoformat(raw_receipt["time_started"]),
                        time_took_us=int(raw_receipt["time_took_us"]),
                        score_str=raw_receipt["score_str"],
                    ),
                    validator_signature=raw_receipt["validator_signature"],
                    miner_signature=raw_receipt["miner_signature"],
                )
            except (ValueError, pydantic.ValidationError):
                logger.warning(f"Miner sent invalid receipt {raw_receipt=}")
                continue

            if receipt.payload.miner_hotkey != hotkey:
                logger.warning(f"Miner sent receipt of a different miner {receipt=}")
                continue

            if not receipt.verify_miner_signature():
                logger.warning(f"Invalid miner signature of receipt {receipt=}")
                continue

            if not receipt.verify_validator_signature():
                logger.warning(f"Invalid validator signature of receipt {receipt=}")
                continue

            receipts.append(receipt)

        return receipts
