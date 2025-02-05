from asyncio import Semaphore
from functools import partial
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest
from pydantic import ValidationError

from compute_horde.receipts.models import JobStartedReceipt
from compute_horde.receipts.transfer import ReceiptsTransfer
from compute_horde.receipts.transfer_checkpoints import TransferCheckpointBackend
from compute_horde.utils import sign_blob
from tests.utils import random_keypair, random_receipt

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
        random_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
        b"not a receipt",
        b"",
        b"",
        random_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
        random_receipt(miner_keypair, validator_keypair).model_dump_json().encode()[50:],
        b"garbage",
        random_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
    ]

    session = Mock()
    session.get = AsyncMock()
    session.get.return_value.status = 200
    session.get.return_value.read = AsyncMock(return_value=b"\n".join(lines_to_serve))

    result = await do_transfer(session=session)

    assert result.n_receipts == 3
    assert len(result.transfer_errors) == 0
    assert len(result.line_errors) == 5
    assert await JobStartedReceipt.objects.acount() == 3


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_rejects_bad_signatures(miner_keypair, validator_keypair):
    bad_miner_signature = random_receipt(miner_keypair, validator_keypair)
    bad_miner_signature.miner_signature = sign_blob(
        random_keypair(),
        bad_miner_signature.payload.blob_for_signing(),
    )

    bad_validator_signature = random_receipt(miner_keypair, validator_keypair)
    bad_validator_signature.validator_signature = sign_blob(
        random_keypair(),
        bad_validator_signature.payload.blob_for_signing(),
    )

    both_bad_signatures = random_receipt(miner_keypair, validator_keypair)
    both_bad_signatures.miner_signature = sign_blob(
        random_keypair(),
        both_bad_signatures.payload.blob_for_signing(),
    )
    both_bad_signatures.validator_signature = sign_blob(
        random_keypair(),
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

    result = await do_transfer(session=session)

    assert result.n_receipts == 0
    assert len(result.transfer_errors) == 0
    assert len(result.line_errors) == 3
    assert await JobStartedReceipt.objects.acount() == 0


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
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
        TimeoutError("Doh!"),
        mock_response_500,
        mock_response_400,
    ]
    session.get = AsyncMock(side_effect=response_effects)

    result = await do_transfer(session=session, pages=[*range(len(response_effects))])

    assert result.n_receipts == 0
    assert len(result.transfer_errors) == len(response_effects)
    assert len(result.line_errors) == 0
    assert mocked_checkpoint_backend.value == 0
    assert await JobStartedReceipt.objects.acount() == 0
    # implicit assertion - none of the exceptions sneak out


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_uses_checkpoints(miner_keypair, validator_keypair, mocked_checkpoint_backend):
    session = Mock()
    session.get = AsyncMock()

    # First response: whole page
    initial_data = b"\n".join(
        [
            random_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
            random_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
            random_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
        ]
    )
    initial_response = Mock()
    initial_response.read = AsyncMock(return_value=initial_data)
    initial_response.status = 200

    # Subsequent response: changes on top of first page
    # This should be a range request with offset == length of previous response
    subsequent_data = b"\n".join(
        [
            random_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
            random_receipt(miner_keypair, validator_keypair).model_dump_json().encode(),
        ]
    )
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

    result = await do_transfer(session=session)
    assert result.n_receipts == 3
    assert len(result.transfer_errors) == 0
    assert len(result.line_errors) == 0
    assert mocked_checkpoint_backend.value == len(initial_data)
    assert await JobStartedReceipt.objects.acount() == 3

    result = await do_transfer(session=session)
    _, get_kwargs = session.get.call_args
    assert get_kwargs["headers"]["Range"] == f"bytes={len(initial_data)}-"
    assert result.n_receipts == 2
    assert len(result.transfer_errors) == 0
    assert len(result.line_errors) == 0
    assert mocked_checkpoint_backend.value == 1337
    assert await JobStartedReceipt.objects.acount() == 5

    result = await do_transfer(session=session)
    _, get_kwargs = session.get.call_args
    assert get_kwargs["headers"]["Range"] == f"bytes={1337}-"
    assert result.n_receipts == 0
    assert len(result.transfer_errors) == 0
    assert len(result.line_errors) == 0
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
