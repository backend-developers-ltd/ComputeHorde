import contextlib
import csv
import io
import logging
import shutil
import tempfile
from abc import ABC
from typing import Iterable, AsyncGenerator, AsyncIterable, Protocol, Mapping

import httpx
import pydantic
import requests

from compute_horde.executor_class import ExecutorClass
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    Receipt,
    ReceiptPayload,
    ReceiptType,
)

logger = logging.getLogger(__name__)


class ReceiptFetchError(Exception):
    pass


Offset = int
CheckpointBackend = Mapping[str, int]


class ReceiptsClient:
    def __init__(self, server_url: str, checkpoint_backend: CheckpointBackend):
        self._receipts_url = server_url.rstrip("/")
        self._checkpoints = checkpoint_backend

    async def page(self, page: int, use_checkpoints: bool = True, yield_invalid: bool = False) -> AsyncIterable[
        Receipt]:
        page_url = f"{self._receipts_url}/{page}.jsonl"

        checkpoint_key = page_url
        checkpoint = self._checkpoints[checkpoint_key] if use_checkpoints else 0
        use_range_request = checkpoint > 0

        if use_range_request:
            # We're re-requesting the page starting from a known offset.
            # Ask for file content that was added after the page was last checked.
            # Also, range request and gzip won't work together.
            # (the range relates to compressed bytes then, which are meaningless here.)
            headers = {
                "Accept-Encoding": "",
                "Range": f"bytes={checkpoint}-",
            }
        else:
            # This is the first time we're requesting this page.
            # This will only receive a gzipped page if it's available.
            # If it's not, this will still work but the raw page file will be sent.
            # httpx inflates the file automatically.
            headers = {
                "Accept-Encoding": "gzip",
            }

        async with httpx.AsyncClient() as client:
            response = await client.get(page_url, headers=headers)

        if response.status_code not in {200, 206}:
            logger.warning(f"Request failed for page {page}: {response.status_code}")
            return 

        for line in response.iter_lines():
            try:
                receipt = Receipt.model_validate_json(line)
            except pydantic.ValidationError:
                logger.warning(
                    f"skipping invalid line: {line[:1000]}{' (...)' if len(line) > 1000 else ''}"
                )
                continue
            if not receipt.verify_validator_signature():
                logger.warning(
                    f"skipping {receipt.payload.receipt_type}:{receipt.payload.job_uuid} "
                    f"- invalid validator signature"
                )
                if not yield_invalid: continue
            if not receipt.verify_miner_signature():
                logger.warning(
                    f"skipping {receipt.payload.receipt_type}:{receipt.payload.job_uuid} "
                    f"- invalid miner signature"
                )
                if not yield_invalid: continue
            yield receipt

        # Save the total page size so that next time we request the page we know what range to request.
        if use_range_request:
            # Range requests return a "content-range" header that contains full size of the file.
            # We ask the server not to gzip range requests - so this is the real size of the file.
            range_header = response.headers["content-range"]
            assert range_header.startswith("bytes ")
            _, total_str = range_header.split("/")
            self._checkpoints[checkpoint_key] = int(total_str)
        else:
            # This is the inflated size (uncompressed). The content length in the headers refers to compressed size.
            self._checkpoints[checkpoint_key] = len(response.content)


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
                receipt_type = ReceiptType(raw_receipt["type"])
                receipt_payload: ReceiptPayload

                match receipt_type:
                    case ReceiptType.JobStartedReceipt:
                        receipt_payload = JobStartedReceiptPayload(
                            job_uuid=raw_receipt["job_uuid"],
                            miner_hotkey=raw_receipt["miner_hotkey"],
                            validator_hotkey=raw_receipt["validator_hotkey"],
                            timestamp=raw_receipt["timestamp"],  # type: ignore[arg-type]
                            executor_class=ExecutorClass(raw_receipt["executor_class"]),
                            max_timeout=int(raw_receipt["max_timeout"]),
                            is_organic=raw_receipt.get("is_organic") == "True",
                            ttl=int(raw_receipt["ttl"]),
                        )

                    case ReceiptType.JobFinishedReceipt:
                        receipt_payload = JobFinishedReceiptPayload(
                            job_uuid=raw_receipt["job_uuid"],
                            miner_hotkey=raw_receipt["miner_hotkey"],
                            validator_hotkey=raw_receipt["validator_hotkey"],
                            timestamp=raw_receipt["timestamp"],  # type: ignore[arg-type]
                            time_started=raw_receipt["time_started"],  # type: ignore[arg-type]
                            time_took_us=int(raw_receipt["time_took_us"]),
                            score_str=raw_receipt["score_str"],
                        )

                    case ReceiptType.JobAcceptedReceipt:
                        receipt_payload = JobAcceptedReceiptPayload(
                            job_uuid=raw_receipt["job_uuid"],
                            miner_hotkey=raw_receipt["miner_hotkey"],
                            validator_hotkey=raw_receipt["validator_hotkey"],
                            timestamp=raw_receipt["timestamp"],  # type: ignore[arg-type]
                            time_accepted=raw_receipt["time_accepted"],  # type: ignore[arg-type]
                            ttl=int(raw_receipt["ttl"]),
                        )

                receipt = Receipt(
                    payload=receipt_payload,
                    validator_signature=raw_receipt["validator_signature"],
                    miner_signature=raw_receipt["miner_signature"],
                )

            except (KeyError, ValueError, pydantic.ValidationError):
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
