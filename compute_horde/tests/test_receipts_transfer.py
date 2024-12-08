import asyncio
import random
from asyncio import Semaphore
from functools import partial
from unittest.mock import AsyncMock, patch, Mock
from uuid import uuid4

import aiohttp
import pytest
from bittensor_wallet import Keypair
from django.utils import timezone
from pydantic import ValidationError

from compute_horde.executor_class import ExecutorClass
from compute_horde.receipts import Receipt, ReceiptType
from compute_horde.receipts.management.commands.debug_create_random_receipts import bt_sign_blob
from compute_horde.receipts.models import JobStartedReceipt
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from compute_horde.receipts.transfer import ReceiptsTransfer
from compute_horde.receipts.transfer_checkpoints import TransferCheckpointBackend

do_transfer = partial(
    ReceiptsTransfer.transfer,
    miners=[("miner-hotkey", "ip-address", 12345)],
    pages=[1],
    semaphore=Semaphore(100),
    request_timeout=100,
)


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_rejects_bad_lines(miner_keypair, validator_keypair):
    lines_to_serve = [
        _create_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
        b"not a receipt",
        b"",
        b"",
        _create_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
        _create_receipt(miner_keypair, validator_keypair).model_dump_json().encode()[50:],
        b"garbage",
        _create_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
    ]

    session = Mock()
    session.get = AsyncMock()
    session.get.return_value.status = 200
    session.get.return_value.read = AsyncMock(return_value=b"\n".join(lines_to_serve))

    n_receipts, n_transfer_errors = await do_transfer(session=session)

    assert n_receipts == 3
    assert n_transfer_errors == 0
    assert await JobStartedReceipt.objects.acount() == 3


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_rejects_bad_signatures(miner_keypair, validator_keypair):
    bad_miner_signature = _create_receipt(miner_keypair, validator_keypair)
    bad_miner_signature.miner_signature = bt_sign_blob(
        _random_keypair(),
        bad_miner_signature.payload.blob_for_signing(),
    )

    bad_validator_signature = _create_receipt(miner_keypair, validator_keypair)
    bad_validator_signature.validator_signature = bt_sign_blob(
        _random_keypair(),
        bad_validator_signature.payload.blob_for_signing(),
    )

    both_bad_signatures = _create_receipt(miner_keypair, validator_keypair)
    both_bad_signatures.miner_signature = bt_sign_blob(
        _random_keypair(),
        both_bad_signatures.payload.blob_for_signing(),
    )
    both_bad_signatures.validator_signature = bt_sign_blob(
        _random_keypair(),
        both_bad_signatures.payload.blob_for_signing(),
    )

    lines_to_serve = [
        bad_miner_signature.model_dump_json().encode(),
        bad_validator_signature.model_dump_json().encode(),
        both_bad_signatures.model_dump_json().encode(),
    ]

    session = Mock()
    session.get = AsyncMock()
    session.get.return_value.status = 200
    session.get.return_value.read = AsyncMock(return_value=b"\n".join(lines_to_serve))

    n_receipts, n_transfer_errors = await do_transfer(session=session)

    assert n_receipts == 0
    assert n_transfer_errors == 0
    assert await JobStartedReceipt.objects.acount() == 0


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_handles_failures(miner_keypair, mocked_checkpoint_backend):
    session = Mock()

    mock_response_500 = Mock()
    mock_response_500.status = 500
    mock_response_400 = Mock()
    mock_response_400.status = 400

    response_effects = [
        Exception("Doh!"),
        ValueError("Doh!"),
        ValidationError("Doh!", []),
        aiohttp.ClientError("Doh!"),
        asyncio.TimeoutError("Doh!"),
        mock_response_500,
        mock_response_400,
    ]
    session.get = AsyncMock(side_effect=response_effects)

    n_receipts, n_transfer_errors = await do_transfer(session=session, pages=[*range(len(response_effects))])

    assert n_receipts == 0
    assert n_transfer_errors == len(response_effects)
    assert mocked_checkpoint_backend.value == 0
    assert await JobStartedReceipt.objects.acount() == 0


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_uses_checkpoints(miner_keypair, validator_keypair, mocked_checkpoint_backend):
    session = Mock()
    session.get = AsyncMock()

    # First response: whole page
    initial_data = b"\n".join([
        _create_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
        _create_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
        _create_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
    ])
    initial_response = Mock()
    initial_response.read = AsyncMock(return_value=initial_data)
    initial_response.status = 200

    # Subsequent response: changes on top of first page
    # This should be a range request with offset == length of previous response
    subsequent_data = b"\n".join([
        _create_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
        _create_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
    ])
    subsequent_response = Mock()
    subsequent_response.status = 206
    subsequent_response.read = AsyncMock(return_value=subsequent_data)
    subsequent_response.headers = {"content-range": "bytes doesntmatter/1337"}

    # Final response: nothing more to serve
    # This should also be a range request with offset == 1337 - taken from the content-range header of previous response
    no_more_content_response = Mock()
    no_more_content_response.status = 416
    no_more_content_response.read = AsyncMock(return_value=b"")

    session.get.side_effect = [
        initial_response,
        subsequent_response,
        no_more_content_response,
    ]

    n_receipts, n_transfer_errors = await do_transfer(session=session)
    assert n_receipts == 3
    assert n_transfer_errors == 0
    assert mocked_checkpoint_backend.value == len(initial_data)
    assert await JobStartedReceipt.objects.acount() == 3

    n_receipts, n_transfer_errors = await do_transfer(session=session)
    _, get_kwargs = session.get.call_args
    assert get_kwargs["headers"]["Range"] == f"bytes={len(initial_data)}-"
    assert n_receipts == 2
    assert n_transfer_errors == 0
    assert mocked_checkpoint_backend.value == 1337
    assert await JobStartedReceipt.objects.acount() == 5

    n_receipts, n_transfer_errors = await do_transfer(session=session)
    _, get_kwargs = session.get.call_args
    assert get_kwargs["headers"]["Range"] == f"bytes={1337}-"
    assert n_receipts == 0
    assert n_transfer_errors == 0
    assert mocked_checkpoint_backend.value == 1337
    assert await JobStartedReceipt.objects.acount() == 5


@pytest.fixture(autouse=True)
def mocked_checkpoint_backend():
    """
    Creates an in-memory single-value checkpoint backend for each test.
    """
    with patch("compute_horde.receipts.transfer.checkpoint_backend") as mocked:
        backend = MockTransferCheckpointBackend()
        mocked.return_value = backend
        yield backend


class MockTransferCheckpointBackend(TransferCheckpointBackend):
    def __init__(self):
        self.value = 0

    async def get(self, key: str) -> int:
        return self.value

    async def set(self, key: str, checkpoint: int) -> None:
        self.value = checkpoint


def _create_receipt(miner: Keypair, validator: Keypair):
    payload = JobStartedReceiptPayload(
        job_uuid=str(uuid4()),
        miner_hotkey=miner.ss58_address,
        validator_hotkey=validator.ss58_address,
        timestamp=timezone.now(),
        receipt_type=ReceiptType.JobStartedReceipt,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        max_timeout=100,
        ttl=100,
        is_organic=False,
    )
    blob = payload.blob_for_signing()
    return Receipt(
        payload=payload,
        miner_signature=bt_sign_blob(miner, blob),
        validator_signature=bt_sign_blob(validator, blob),
    )


def _random_keypair() -> Keypair:
    return Keypair.create_from_seed("".join(random.sample("0123456789abcdef" * 10, 64)))
