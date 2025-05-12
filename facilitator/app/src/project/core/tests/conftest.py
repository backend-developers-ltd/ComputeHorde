import base64
import time
from importlib import import_module

import pytest
import pytest_asyncio
import responses
from bittensor import Keypair
from bittensor_wallet import Wallet
from channels.testing import WebsocketCommunicator
from compute_horde.fv_protocol.validator_requests import (
    JobStatusMetadata,
    JobStatusUpdate,
    MinerResponse,
    V0AuthenticationRequest,
)
from compute_horde_core.signature import Signature
from django.conf import settings
from django.contrib.auth.models import User

from ...asgi import application
from ..models import Channel, Job, Validator

for package in settings.ADDITIONAL_APPS:
    module = import_module(f"{package}.tests.conftest")
    for name in dir(module):
        if not name.startswith("_"):
            locals()[name] = getattr(module, name)


@pytest_asyncio.fixture
async def communicator():
    communicator = WebsocketCommunicator(application, "/ws/v0/")
    connected, _ = await communicator.connect()
    assert connected
    yield communicator
    await communicator.disconnect(200)


@pytest.fixture
def keypair():
    return Keypair.create_from_mnemonic("slot excuse valid grief praise rifle spoil auction weasel glove pen share")


@pytest.fixture
def public_key(keypair):
    assert keypair.public_key
    return keypair.public_key


@pytest.fixture
def wallet():
    wallet = Wallet(name="test-wallet")
    wallet.create_if_non_existent(
        coldkey_use_password=False,
        hotkey_use_password=False,
        save_coldkey_to_env=False,
        save_hotkey_to_env=False,
    )
    return wallet


@pytest.fixture
def authentication_request(keypair):
    return V0AuthenticationRequest.from_keypair(keypair)


@pytest_asyncio.fixture
async def authenticated(communicator, authentication_request, validator):
    """Already authenticated communicator"""
    await communicator.send_json_to(authentication_request.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "success", response


@pytest.fixture
def other_signature(keypair):
    other_keypair = Keypair.create_from_mnemonic(
        "lion often fade hover duty debris write tumble shock ask bracket roast"
    )
    return f"0x{other_keypair.sign(keypair.public_key).hex()}"


@pytest.fixture
def validator(db, keypair):
    return Validator.objects.create(ss58_address=keypair.ss58_address, is_active=True)


@pytest.fixture
def connected_validator(db, validator):
    Channel.objects.create(name="test", validator=validator)
    return validator


@pytest.fixture
def user(db):
    return User.objects.create(username="testuser", email="test@localhost")


@pytest.fixture
def signature():
    return Signature(
        signature_type="dummy_signature_type",
        signatory="dummy_signatory",
        timestamp_ns=time.time_ns(),
        signature=base64.b64encode(b"dummy_signature"),
    )


@pytest.fixture
def dummy_job_params(settings):
    return dict(
        docker_image="",
        args=["arg1", "value1"],
        env={"ENV1": "VALUE1"},
    )


@pytest.fixture
def job(db, user, validator, signature, dummy_job_params):
    return Job.objects.create(
        user=user,
        validator=validator,
        target_validator_hotkey=validator.ss58_address,
        signature=signature.model_dump(),
        **dummy_job_params,
    )


@pytest.fixture
def job_with_hotkey(job, wallet):
    job.user = None
    job.hotkey = wallet.hotkey.ss58_address
    job.save()
    return job


@pytest_asyncio.fixture
async def ignore_job_request(communicator, job):
    """Make validator ignore job request from the app"""

    response = await communicator.receive_json_from()
    assert response["type"] == "job.new"


@pytest.fixture
def job_status_update(job):
    return JobStatusUpdate(
        uuid=str(job.uuid),
        status="accepted",
        metadata=JobStatusMetadata(
            comment="some comment",
            miner_response=MinerResponse(
                job_uuid=str(job.uuid),
                message_type="some-type",
                docker_process_stderr="some stderr",
                docker_process_stdout="some stdout",
                artifacts={},
                upload_results={},
            ),
        ),
    )


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps
