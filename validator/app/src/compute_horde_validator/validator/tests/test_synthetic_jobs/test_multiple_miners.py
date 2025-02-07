import asyncio
import re
import uuid
from collections.abc import Callable
from datetime import timedelta
from unittest.mock import patch

import bittensor
import httpx
import pytest
import pytest_asyncio
from compute_horde.certificate import generate_certificate_at
from compute_horde.executor_class import (
    DEFAULT_EXECUTOR_CLASS,
    DEFAULT_LLM_EXECUTOR_CLASS,
    ExecutorClass,
)
from compute_horde.miner_client.base import AbstractTransport
from compute_horde.mv_protocol import miner_requests
from compute_horde.receipts import Receipt
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.utils import sign_blob
from constance.test import override_config
from django.utils import timezone
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    SyntheticJob,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.s3 import get_public_url
from compute_horde_validator.validator.synthetic_jobs.batch_run import (
    BatchContext,
    MinerClient,
    execute_synthetic_batch_run,
)
from compute_horde_validator.validator.tests.transport import SimulationTransport

from ...synthetic_jobs.generator import llm_prompts
from .helpers import check_miner_job_system_events, check_synthetic_job, generate_prompts
from .mock_generator import NOT_SCORED, LlmPromptsSyntheticJobGeneratorFactory

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(databases=["default", "default_alias"], transaction=True),
]

MOCK_EXCUSED_SCORE = 1.337


@pytest.fixture
def num_miners():
    return 8


@pytest.fixture
def job_uuids(num_miners: int):
    return [uuid.uuid4() for _ in range(num_miners)]


@pytest.fixture
def miner_wallets(num_miners: int):
    return [bittensor.Keypair.create_from_seed(f"abc{i}".ljust(64, "f")) for i in range(num_miners)]


@pytest.fixture
def miner_hotkeys(miner_wallets: list[bittensor.Keypair]):
    return [k.ss58_address for k in miner_wallets]


@pytest.fixture
def active_valis() -> list[bittensor.Keypair]:
    return [
        bittensor.Keypair.create_from_seed("a" * 64),
        bittensor.Keypair.create_from_seed("b" * 64),
        bittensor.Keypair.create_from_seed("c" * 64),
    ]


@pytest.fixture
def inactive_valis() -> list[bittensor.Keypair]:
    return [
        bittensor.Keypair.create_from_seed("d" * 64),
        bittensor.Keypair.create_from_seed("e" * 64),
        bittensor.Keypair.create_from_seed("f" * 64),
    ]


@pytest_asyncio.fixture
async def miners(miner_hotkeys: list[str]):
    objs = [Miner(hotkey=hotkey) for hotkey in miner_hotkeys]
    await Miner.objects.abulk_create(objs)
    return objs


@pytest.fixture
def miner_axon_infos(miner_hotkeys: str):
    return [
        bittensor.AxonInfo(
            version=4,
            ip="ignore",
            ip_type=4,
            port=9999,
            hotkey=hotkey,
            coldkey=hotkey,
        )
        for hotkey in miner_hotkeys
    ]


@pytest.fixture
def axon_dict(miner_axon_infos: list[bittensor.AxonInfo]):
    return {axon.hotkey: axon for axon in miner_axon_infos}


@pytest_asyncio.fixture
async def transports(miner_hotkeys: str):
    return [SimulationTransport(hotkey) for hotkey in miner_hotkeys]


@pytest.fixture
def create_simulation_miner_client(miner_hotkeys: list[str], transports: list[AbstractTransport]):
    transport_dict = {hotkey: transport for hotkey, transport in zip(miner_hotkeys, transports)}

    def _create(ctx: BatchContext, miner_hotkey: str):
        return MinerClient(
            ctx=ctx, miner_hotkey=miner_hotkey, transport=transport_dict[miner_hotkey]
        )

    return _create


@pytest.fixture
def ssl_public_key():
    return generate_certificate_at()[1]


async def test_all_succeed(
    axon_dict: dict[str, bittensor.AxonInfo],
    transports: list[SimulationTransport],
    miners: list[Miner],
    create_simulation_miner_client: Callable,
    job_uuids: list[uuid.UUID],
    manifest_message: str,
):
    for job_uuid, transport in zip(job_uuids, transports):
        await transport.add_message(manifest_message, send_before=1)

        accept_message = miner_requests.V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()
        await transport.add_message(accept_message, send_before=1)

        executor_ready_message = miner_requests.V0ExecutorReadyRequest(
            job_uuid=str(job_uuid)
        ).model_dump_json()
        await transport.add_message(executor_ready_message, send_before=0)

        job_finish_message = miner_requests.V0JobFinishedRequest(
            job_uuid=str(job_uuid), docker_process_stdout="", docker_process_stderr=""
        ).model_dump_json()

        await transport.add_message(job_finish_message, send_before=2)

    batch = await SyntheticJobBatch.objects.acreate(
        block=1000,
        cycle=await Cycle.objects.acreate(start=708, stop=1430),
    )
    await asyncio.wait_for(
        execute_synthetic_batch_run(
            axon_dict,
            miners,
            [],
            batch.id,
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=1,
    )

    for job_uuid, miner in zip(job_uuids, miners):
        await check_synthetic_job(job_uuid, miner.pk, SyntheticJob.Status.COMPLETED, 1)


async def prep_mocks_for_streaming(
    mocker: MockerFixture, httpx_mock: HTTPXMock, job_uuids: list[uuid.UUID], settings
):
    prompts, prompt_samples = await generate_prompts(num_miners=len(job_uuids))
    mocker.patch(
        "compute_horde_validator.validator.synthetic_jobs.batch_run.get_streaming_job_executor_classes",
        return_value={DEFAULT_LLM_EXECUTOR_CLASS},
    )
    mocker.patch(
        "compute_horde_validator.validator.synthetic_jobs.generator.current.synthetic_job_generator_factory",
        LlmPromptsSyntheticJobGeneratorFactory(
            uuids=job_uuids.copy(),
            prompt_samples=prompt_samples,
            prompts=prompts,
            streaming=True,
        ),
    )
    mocker.patch.object(llm_prompts, "STREAMING_PROCESSING_TIMEOUT", 1)
    mocker.patch.object(llm_prompts, "STREAMING_PROCESSING_TIMEOUT_LEEWAY", 0.5)

    httpx_mock.add_response(
        url=re.compile(
            get_public_url(key=".*", bucket_name=settings.S3_BUCKET_NAME_ANSWERS, prefix="solved/")
        ),
        json={p.content: p.answer for p in prompts},
    )

    async def sleepy_request(*_):
        await asyncio.sleep(2)
        return httpx.Response(201)

    httpx_mock.add_callback(sleepy_request, url=re.compile("https://127.0.0.1:8007.*"))


@pytest.mark.override_config(
    DYNAMIC_SYNTHETIC_STREAMING_JOB_READY_TIMEOUT=0.5,
)
async def test_some_streaming_succeed(
    axon_dict: dict[str, bittensor.AxonInfo],
    transports: list[SimulationTransport],
    miners: list[Miner],
    create_simulation_miner_client: Callable,
    job_uuids: list[uuid.UUID],
    streaming_manifest_message: str,
    httpx_mock: HTTPXMock,
    mocker: MockerFixture,
    ssl_public_key: str,
    settings,
):
    await prep_mocks_for_streaming(mocker, httpx_mock, job_uuids, settings)
    # generator will solve to the right answer
    MOCK_SCORE = 1.0

    port = 8000
    for job_uuid, transport in zip(job_uuids, transports):
        await transport.add_message(streaming_manifest_message, send_before=1)

        accept_message = miner_requests.V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()
        await transport.add_message(accept_message, send_before=1)

        executor_ready_message = miner_requests.V0ExecutorReadyRequest(
            job_uuid=str(job_uuid)
        ).model_dump_json()
        await transport.add_message(executor_ready_message, send_before=0)

        if job_uuid != job_uuids[-1]:
            streaming_ready_message = miner_requests.V0StreamingJobReadyRequest(
                job_uuid=str(job_uuid),
                public_key=ssl_public_key,
                ip="127.0.0.1",
                port=(port := port + 1),
            ).model_dump_json()
            await transport.add_message(streaming_ready_message, send_before=0)

            job_finish_message = miner_requests.V0JobFinishedRequest(
                job_uuid=str(job_uuid), docker_process_stdout="", docker_process_stderr=""
            ).model_dump_json()

            await transport.add_message(job_finish_message, send_before=2)

    batch = await SyntheticJobBatch.objects.acreate(
        block=1000,
        cycle=await Cycle.objects.acreate(start=708, stop=1430),
    )
    await asyncio.wait_for(
        execute_synthetic_batch_run(
            axon_dict,
            miners,
            [],
            batch.id,
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=10,
    )

    for job_uuid, miner in zip(job_uuids, miners):
        if job_uuid == job_uuids[-1]:
            await check_synthetic_job(job_uuid, miner.pk, SyntheticJob.Status.FAILED, 0)
        elif job_uuid == job_uuids[-2]:
            await check_synthetic_job(
                job_uuid,
                miner.pk,
                SyntheticJob.Status.FAILED,
                0,
                re.compile("took too long: time_took_sec=.*"),
            )
        else:
            await check_synthetic_job(job_uuid, miner.pk, SyntheticJob.Status.COMPLETED, MOCK_SCORE)


@pytest_asyncio.fixture
async def flow_0(
    transports: list[SimulationTransport], manifest_message: str, job_uuids: list[uuid.UUID]
):
    """
    Job successfully finished
    """

    index = 0
    transport = transports[index]
    job_uuid = job_uuids[index]

    await transport.add_message(manifest_message, send_before=1)

    accept_message = miner_requests.V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()
    await transport.add_message(accept_message, send_before=1)

    executor_ready_message = miner_requests.V0ExecutorReadyRequest(
        job_uuid=str(job_uuid)
    ).model_dump_json()
    await transport.add_message(executor_ready_message, send_before=0)

    job_finish_message = miner_requests.V0JobFinishedRequest(
        job_uuid=str(job_uuid), docker_process_stdout="", docker_process_stderr=""
    ).model_dump_json()

    await transport.add_message(job_finish_message, send_before=2)


@pytest_asyncio.fixture
async def flow_1(
    transports: list[SimulationTransport], manifest_message: str, job_uuids: list[uuid.UUID]
):
    """
    Job timed out
    """

    index = 1
    transport = transports[index]
    job_uuid = job_uuids[index]

    await transport.add_message(manifest_message, send_before=1)

    accept_message = miner_requests.V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()
    await transport.add_message(accept_message, send_before=1)

    executor_ready_message = miner_requests.V0ExecutorReadyRequest(
        job_uuid=str(job_uuid)
    ).model_dump_json()
    await transport.add_message(executor_ready_message, send_before=0)

    job_finish_message = miner_requests.V0JobFinishedRequest(
        job_uuid=str(job_uuid), docker_process_stdout="", docker_process_stderr=""
    ).model_dump_json()

    await transport.add_message(job_finish_message, send_before=2, sleep_before=2)


@pytest_asyncio.fixture
async def flow_2(
    transports: list[SimulationTransport], manifest_message: str, job_uuids: list[uuid.UUID]
):
    """
    Job failed
    """

    index = 2
    transport = transports[index]
    job_uuid = job_uuids[index]

    await transport.add_message(manifest_message, send_before=1)

    accept_message = miner_requests.V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()
    await transport.add_message(accept_message, send_before=1)

    executor_ready_message = miner_requests.V0ExecutorReadyRequest(
        job_uuid=str(job_uuid)
    ).model_dump_json()
    await transport.add_message(executor_ready_message, send_before=0)

    job_failed_message = miner_requests.V0JobFailedRequest(
        job_uuid=str(job_uuid), docker_process_stdout="", docker_process_stderr=""
    ).model_dump_json()

    await transport.add_message(job_failed_message, send_before=2)


@pytest_asyncio.fixture
async def flow_3(
    transports: list[SimulationTransport], manifest_message: str, job_uuids: list[uuid.UUID]
):
    """
    Job declined - no reason
    """

    index = 3
    transport = transports[index]
    job_uuid = job_uuids[index]

    await transport.add_message(manifest_message, send_before=1)

    decline_message = miner_requests.V0DeclineJobRequest(job_uuid=str(job_uuid)).model_dump_json()
    await transport.add_message(decline_message, send_before=1)


@pytest_asyncio.fixture
async def flow_4(
    transports: list[SimulationTransport], manifest_message: str, job_uuids: list[uuid.UUID]
):
    """
    Job declined - busy, but no receipts provided.
    """

    index = 4
    transport = transports[index]
    job_uuid = job_uuids[index]

    await transport.add_message(manifest_message, send_before=1)

    decline_message = miner_requests.V0DeclineJobRequest(
        job_uuid=str(job_uuid),
        reason=miner_requests.V0DeclineJobRequest.Reason.BUSY,
    ).model_dump_json()
    await transport.add_message(decline_message, send_before=1)


@pytest_asyncio.fixture
async def flow_5(
    transports: list[SimulationTransport],
    manifest_message: str,
    job_uuids: list[uuid.UUID],
    active_valis: list[bittensor.Keypair],
    inactive_valis: list[bittensor.Keypair],
    miner_wallets: list[bittensor.Keypair],
):
    """
    Job declined - busy, bad excuses provided.
    """

    index = 5
    transport = transports[index]
    job_uuid = job_uuids[index]
    miner_wallet = miner_wallets[index]

    await transport.add_message(manifest_message, send_before=1)

    decline_message = miner_requests.V0DeclineJobRequest(
        job_uuid=str(job_uuid),
        reason=miner_requests.V0DeclineJobRequest.Reason.BUSY,
        receipts=_build_invalid_excuse_receipts(
            active_valis[0], miner_wallet, inactive_valis[0], job_uuid
        ),
    ).model_dump_json()
    await transport.add_message(decline_message, send_before=1)


@pytest_asyncio.fixture
async def flow_6(
    transports: list[SimulationTransport],
    manifest_message: str,
    job_uuids: list[uuid.UUID],
    active_valis: list[bittensor.Keypair],
    inactive_valis: list[bittensor.Keypair],
    miner_wallets: list[bittensor.Keypair],
):
    """
    Job declined - busy, good excuse.
    """

    index = 6
    transport = transports[index]
    job_uuid = job_uuids[index]
    miner_wallet = miner_wallets[index]

    await transport.add_message(manifest_message, send_before=1)

    excuse = JobStartedReceiptPayload(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_wallet.ss58_address,
        validator_hotkey=active_valis[0].ss58_address,
        timestamp=timezone.now() - timedelta(seconds=10),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        max_timeout=123,
        is_organic=True,
        ttl=60,
    )
    excuse_blob = excuse.blob_for_signing()

    decline_message = miner_requests.V0DeclineJobRequest(
        job_uuid=str(job_uuid),
        reason=miner_requests.V0DeclineJobRequest.Reason.BUSY,
        receipts=[
            Receipt(
                payload=excuse,
                validator_signature=sign_blob(active_valis[0], excuse_blob),
                miner_signature=sign_blob(miner_wallet, excuse_blob),
            )
        ],
    ).model_dump_json()
    await transport.add_message(decline_message, send_before=1)


@pytest_asyncio.fixture
async def flow_7():
    """
    No manifest. Fixture just for indication
    """


@pytest.fixture(autouse=True)
def mock_excuse_score():
    with override_config(DYNAMIC_EXCUSED_SYNTHETIC_JOB_SCORE=MOCK_EXCUSED_SCORE):
        yield


@patch("compute_horde_validator.validator.synthetic_jobs.batch_run._GET_MANIFEST_TIMEOUT", 0.2)
@patch(
    "compute_horde_validator.validator.synthetic_jobs.batch_run._JOB_RESPONSE_EXTRA_TIMEOUT", 0.1
)
@patch("compute_horde_validator.validator.synthetic_jobs.batch_run.random.shuffle", lambda x: x)
async def test_complex(
    axon_dict: dict[str, bittensor.AxonInfo],
    miners: list[Miner],
    transports,
    create_simulation_miner_client: Callable,
    job_uuids: list[uuid.UUID],
    flow_0,
    flow_1,
    flow_2,
    flow_3,
    flow_4,
    flow_5,
    flow_6,
    flow_7,
    active_valis: list[bittensor.Keypair],
):
    for transport, miner in zip(transports, miners):
        assert transport.name == miner.hotkey

    batch = await SyntheticJobBatch.objects.acreate(
        block=1000,
        cycle=await Cycle.objects.acreate(start=708, stop=1430),
    )
    await asyncio.wait_for(
        execute_synthetic_batch_run(
            axon_dict,
            miners,
            [v.ss58_address for v in active_valis],
            batch.id,
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=2,
    )

    assert await SyntheticJob.objects.acount() == 7
    assert (
        await SystemEvent.objects.exclude(type=SystemEvent.EventType.VALIDATOR_TELEMETRY).acount()
        == 8
    )

    await check_synthetic_job(job_uuids[0], miners[0].pk, SyntheticJob.Status.COMPLETED, 1)
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS,
                SystemEvent.EventSubType.SUCCESS,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[0].hotkey,
        job_uuids[0],
    )

    await check_synthetic_job(job_uuids[1], miners[1].pk, SyntheticJob.Status.FAILED, NOT_SCORED)
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[1].hotkey,
        job_uuids[1],
    )

    await check_synthetic_job(job_uuids[2], miners[2].pk, SyntheticJob.Status.FAILED, NOT_SCORED)
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.FAILURE,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[2].hotkey,
        job_uuids[2],
    )

    await check_synthetic_job(job_uuids[3], miners[3].pk, SyntheticJob.Status.FAILED, NOT_SCORED)
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.JOB_REJECTED,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[3].hotkey,
        job_uuids[3],
    )

    await check_synthetic_job(job_uuids[4], miners[4].pk, SyntheticJob.Status.FAILED, NOT_SCORED)
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.JOB_REJECTED,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[4].hotkey,
        job_uuids[4],
    )

    await check_synthetic_job(job_uuids[5], miners[5].pk, SyntheticJob.Status.FAILED, NOT_SCORED)
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.JOB_REJECTED,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[5].hotkey,
        job_uuids[5],
    )

    await check_synthetic_job(
        job_uuids[6], miners[6].pk, SyntheticJob.Status.EXCUSED, MOCK_EXCUSED_SCORE
    )
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.JOB_EXCUSED,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[6].hotkey,
        job_uuids[6],
    )
    excused_event = await SystemEvent.objects.aget(
        type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
        subtype=SystemEvent.EventSubType.JOB_EXCUSED,
        data__miner_hotkey=miners[6].hotkey,
        data__job_uuid=str(job_uuids[6]),
    )
    assert excused_event.data["excused_by"] == [active_valis[0].ss58_address]

    # Check batch telemetry counts
    telemetry = await SystemEvent.objects.aget(
        type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
        subtype=SystemEvent.EventSubType.SYNTHETIC_BATCH,
    )
    assert telemetry.data["counts"]["jobs"] == {
        "total": 7,
        "failed": 5,
        "correct": 1,
        "excused": 1,
        "incorrect": 0,
        "successful": 1,
    }
    assert (
        telemetry.data["counts"]["jobs"]
        == telemetry.data["counts"]["jobs:" + DEFAULT_EXECUTOR_CLASS]
    )

    # TODO: Make this system event bound to the miner and the job
    assert (
        await SystemEvent.objects.filter(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.MANIFEST_TIMEOUT,
        ).acount()
        == 1
    )


def _build_invalid_excuse_receipts(
    validator: bittensor.Keypair,
    miner: bittensor.Keypair,
    bad_validator: bittensor.Keypair,
    job: uuid.UUID,
) -> list[Receipt]:
    good_payload = JobStartedReceiptPayload(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner.ss58_address,
        validator_hotkey=validator.ss58_address,
        timestamp=timezone.now(),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        max_timeout=123,
        is_organic=True,
        ttl=60,
    )
    good_payload_blob = good_payload.blob_for_signing()

    bad_receipt_type_1 = JobAcceptedReceiptPayload(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner.ss58_address,
        validator_hotkey=validator.ss58_address,
        timestamp=timezone.now(),
        time_accepted=timezone.now(),
        ttl=60,
    )

    bad_receipt_type_2 = JobFinishedReceiptPayload(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner.ss58_address,
        validator_hotkey=validator.ss58_address,
        timestamp=timezone.now(),
        time_started=timezone.now(),
        time_took_us=50000,
        score_str="123",
    )

    non_organic = good_payload.__replace__(is_organic=False)
    other_miner = good_payload.__replace__(
        miner_hotkey=bittensor.Keypair.create_from_seed("7" * 64).ss58_address
    )
    same_job = good_payload.__replace__(job_uuid=str(job))
    bad_executor_class = good_payload.__replace__(
        executor_class=next(c for c in ExecutorClass if c != DEFAULT_EXECUTOR_CLASS)
    )
    future_receipt = good_payload.__replace__(timestamp=timezone.now() + timedelta(minutes=5))
    expired_receipt = good_payload.__replace__(timestamp=timezone.now() - timedelta(minutes=5))

    receipts: list[Receipt] = []

    for payload in [
        bad_receipt_type_1,
        bad_receipt_type_2,
        non_organic,
        other_miner,
        same_job,
        bad_executor_class,
        future_receipt,
        expired_receipt,
    ]:
        blob = payload.blob_for_signing()
        receipt = Receipt(
            payload=payload,
            validator_signature=sign_blob(validator, blob),
            miner_signature=sign_blob(miner, blob),
        )
        receipts.append(receipt)

    # Bad vali signature
    receipts.append(
        Receipt(
            payload=good_payload,
            validator_signature=sign_blob(validator, good_payload_blob)[:-6] + "foobar",
            miner_signature=sign_blob(miner, good_payload_blob),
        )
    )

    # Inactive validator
    receipts.append(
        Receipt(
            payload=good_payload,
            validator_signature=sign_blob(bad_validator, good_payload_blob),
            miner_signature=sign_blob(miner, good_payload_blob),
        )
    )

    return receipts
