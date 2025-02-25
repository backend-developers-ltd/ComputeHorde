from datetime import timedelta
from importlib import import_module

import pytest
import pytest_asyncio
import responses
from bittensor import Keypair
from bittensor_wallet import Wallet
from channels.testing import WebsocketCommunicator
from compute_horde.fv_protocol.validator_requests import V0AuthenticationRequest
from django.conf import settings
from django.contrib.auth.models import User
from django.utils.timezone import now
from freezegun import freeze_time

from ...asgi import application
from ..models import Channel, Job, JobStatus, Miner, Validator
from ..schemas import JobStatusMetadata, JobStatusUpdate, MinerResponse

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
def miner(db, keypair):
    return Miner.objects.create(ss58_address=keypair.ss58_address, is_active=True)


@pytest.fixture
def miners(db):
    return Miner.objects.bulk_create([Miner(ss58_address=f"miner{i}", is_active=True) for i in range(10)])


@pytest.fixture
def user(db):
    return User.objects.create(username="testuser", email="test@localhost")


@pytest.fixture
def dummy_job_params(settings):
    return dict(
        docker_image="",
        raw_script="import this",
        args="arg1 value1",
        env={"ENV1": "VALUE1"},
        input_url="http://localhost/input",
        hf_repo_id="http://localhost/hf",
        hf_revision="123456",
        output_upload_url="http://localhost/output/upload",
        output_download_url="http://localhost/output/download",
        output_download_url_expires_at=now() + settings.DOWNLOAD_PRESIGNED_URL_LIFETIME,
    )


@pytest.fixture
def job(db, user, validator, miner, dummy_job_params):
    return Job.objects.create(
        user=user,
        validator=validator,
        miner=miner,
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
            ),
        ),
    )


@pytest.fixture
def inactive_miner(db):
    return Miner.objects.create(ss58_address="inactive-miner", is_active=False)


@pytest_asyncio.fixture
async def active_miner_with_executing_job(db, user, validator, dummy_job_params):
    miner = await Miner.objects.acreate(ss58_address="miner-with-active-job", is_active=True)

    # add some old completed jobs
    with freeze_time(now() - timedelta(hours=1)):
        for _ in range(20):
            job = await Job.objects.acreate(
                user=user,
                validator=validator,
                miner=miner,
                **dummy_job_params,
            )
            await JobStatus.objects.acreate(job=job, status=JobStatus.Status.ACCEPTED)
            await JobStatus.objects.acreate(job=job, status=JobStatus.Status.COMPLETED)

    job = await Job.objects.acreate(user=user, validator=validator, miner=miner, **dummy_job_params)
    await JobStatus.objects.acreate(job=job, status=JobStatus.Status.ACCEPTED)

    return miner


@pytest_asyncio.fixture
async def miner_with_recently_completed_jobs(db, user, validator, dummy_job_params):
    miner = await Miner.objects.acreate(ss58_address="miner-with-recently-completed-jobs", is_active=True)

    for _ in range(20):
        job = await Job.objects.acreate(
            user=user,
            validator=validator,
            miner=miner,
            **dummy_job_params,
        )
        await JobStatus.objects.acreate(job=job, status=JobStatus.Status.ACCEPTED)
        await JobStatus.objects.acreate(job=job, status=JobStatus.Status.COMPLETED)

    return miner


@pytest_asyncio.fixture
async def miner_with_old_completed_jobs(db, user, validator, dummy_job_params):
    miner = await Miner.objects.acreate(ss58_address="miner-with-old-completed-jobs", is_active=True)

    with freeze_time(now() - timedelta(hours=1)):
        for _ in range(20):
            job = await Job.objects.acreate(
                user=user,
                validator=validator,
                miner=miner,
                **dummy_job_params,
            )
            await JobStatus.objects.acreate(job=job, status=JobStatus.Status.ACCEPTED)
            await JobStatus.objects.acreate(job=job, status=JobStatus.Status.COMPLETED)

    return miner


@pytest_asyncio.fixture
async def miner_with_old_active_jobs(db, user, validator, dummy_job_params):
    miner = await Miner.objects.acreate(ss58_address="miner-with-old-active-jobs", is_active=True)

    with freeze_time(now() - timedelta(hours=1)):
        for _ in range(20):
            job = await Job.objects.acreate(
                user=user,
                validator=validator,
                miner=miner,
                **dummy_job_params,
            )
            await JobStatus.objects.acreate(job=job, status=JobStatus.Status.ACCEPTED)

    return miner


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps
