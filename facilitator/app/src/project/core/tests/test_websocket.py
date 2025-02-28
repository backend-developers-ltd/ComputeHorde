import asyncio
from unittest import mock

import pytest
from bittensor import Keypair
from channels.testing import WebsocketCommunicator
from compute_horde.fv_protocol.validator_requests import V0AuthenticationRequest
from more_itertools import one

from ...asgi import application
from ..models import Job, JobStatus, Validator


@pytest.mark.asyncio
async def test__websocket__unrecognized_message(communicator) -> None:
    """Check response to an unrecognized message"""

    await communicator.send_json_to({"hello": "world"})
    response = await communicator.receive_json_from()
    assert response["status"] == "error"
    expected_error_types = {error["type"] for error in response["errors"]}
    assert {"union_tag_not_found"} == expected_error_types


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__authentication__validator_not_found(communicator, public_key, authentication_request):
    """Check authentication failure in case of unknown validator"""

    await communicator.send_json_to(authentication_request.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "error"
    assert response["errors"] == [
        {
            "msg": "Validator not found",
            "type": "auth.validator_not_found",
            "help": mock.ANY,
        },
    ]


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__authentication__bad_signature(
    communicator, authentication_request, validator, other_signature
):
    """Check authentication failure in case of wrong signature"""

    authentication_request.signature = other_signature
    await communicator.send_json_to(authentication_request.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "error"
    assert response["errors"] == [
        {
            "msg": "Invalid signature",
            "type": "auth.signature_invalid",
            "help": mock.ANY,
        },
    ]


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__authentication__success(communicator, authentication_request, validator):
    """Check legit authentication"""

    await communicator.send_json_to(authentication_request.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "success"
    assert response["errors"] == []


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__double_authentication(communicator, authentication_request, validator):
    """Do not allow authenticating twice within one connection"""

    await communicator.send_json_to(authentication_request.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "success"

    await communicator.send_json_to(authentication_request.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "error"
    assert response["errors"] == [
        {
            "msg": "Already authenticated",
            "type": "auth.already_authenticated",
            "help": mock.ANY,
        },
    ]


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__action__not_authenticated(communicator, validator, job_status_update):
    """Don't allow sending job status message without authentication"""

    await communicator.send_json_to(job_status_update.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "error"
    assert response["errors"] == [
        {
            "msg": "Not authenticated",
            "type": "auth.not_authenticated",
            "help": mock.ANY,
        },
    ]


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__submit_job_status__legit(communicator, authenticated, job_status_update, ignore_job_request):
    """Check sending job status message after authentication"""

    # job status: 'accepted'
    await communicator.send_json_to(job_status_update.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "success"
    status = await JobStatus.objects.select_related("job").order_by("created_at").alast()
    assert str(status.job.pk) == job_status_update.uuid
    assert status.get_status_display().lower() == job_status_update.status

    # job status: 'completed'
    job_status_update.status = "completed"
    await communicator.send_json_to(job_status_update.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "success"
    status = await JobStatus.objects.select_related("job").order_by("created_at").alast()
    assert str(status.job.pk) == job_status_update.uuid
    assert status.get_status_display().lower() == job_status_update.status


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__submit_job_status__duplicate(
    communicator, authenticated, job_status_update, ignore_job_request
):
    """Check sending job status message after authentication"""

    await communicator.send_json_to(job_status_update.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "success"

    await communicator.send_json_to(job_status_update.dict())
    response = await communicator.receive_json_from()
    assert response["status"] == "error"
    assert one(response["errors"])["type"] == "job.integrity_error"


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__new_job__no_broadcasting__unauth(communicator, job):
    """Check that new job message is not broadcasted to unauthorized connections"""

    with pytest.raises(asyncio.TimeoutError):
        await communicator.receive_json_from()


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__new_job__received(communicator, authenticated, job):
    """Check that new job message is received by connected validator"""

    received_job_request = await communicator.receive_json_from()
    assert received_job_request == job.as_job_request().dict()


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__new_job__evenly_distributed(
    communicator,
    authenticated,
    validator,
    miner,
    user,
    dummy_job_params,
):
    """Check that new jobs are received evenly by different validators"""

    # create a second validator
    keypair2 = Keypair.create_from_mnemonic("lion often fade hover duty debris write tumble shock ask bracket roast")
    auth_request2 = V0AuthenticationRequest.from_keypair(keypair2)

    validator2 = await Validator.objects.acreate(ss58_address=keypair2.ss58_address, is_active=True)

    communicator2 = WebsocketCommunicator(application, "/ws/v0/")
    connected, _ = await communicator2.connect()
    assert connected
    await communicator2.send_json_to(auth_request2.dict())
    response = await communicator2.receive_json_from()
    assert response["status"] == "success", response

    # submit many jobs and inspect distribution
    assert await Validator.objects.filter(is_active=True).acount() == 2

    for _ in range(10):
        await Job.objects.acreate(
            user=user,
            miner=miner,
            **dummy_job_params,
        )

    validators_selected = []
    async for id_ in Job.objects.order_by("created_at").values_list("validator_id", flat=True):
        validators_selected.append(id_)

    assert (
        validators_selected == [validator.pk, validator2.pk] * 5
        or validators_selected == [validator2.pk, validator.pk] * 5
    )

    await communicator2.disconnect(200)
