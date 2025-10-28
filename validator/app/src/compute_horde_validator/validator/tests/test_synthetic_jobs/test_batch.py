import asyncio
import json
import math
import uuid
from collections.abc import Callable

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0ExecutorManifestRequest,
    V0ExecutorReadyRequest,
    V0JobFinishedRequest,
)
from compute_horde.subtensor import get_peak_cycle

from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    MinerManifest,
    SyntheticJob,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.synthetic_jobs.batch_run import execute_synthetic_batch_run
from compute_horde_validator.validator.tests.transport import SimulationTransport

from .mock_generator import (
    TimeTookScoreMockSyntheticJobGeneratorFactory,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(databases=["default", "default_alias"], transaction=True),
]


@pytest.fixture
def job_generator_factory():
    return TimeTookScoreMockSyntheticJobGeneratorFactory()


# TODO: refactor the tests in this file to reduce code duplication


async def test_synthetic_job_batch(
    job_generator_factory: TimeTookScoreMockSyntheticJobGeneratorFactory,
    miner: Miner,
    create_simulation_miner_client: Callable,
    transport: SimulationTransport,
    small_spin_up_times,
    override_weights_version_v2,
):
    executor_count = 10
    job_uuids = [uuid.uuid4() for _ in range(executor_count)]
    job_generator_factory._uuids = job_uuids.copy()

    manifest_message = V0ExecutorManifestRequest(
        manifest={DEFAULT_EXECUTOR_CLASS: executor_count}
    ).model_dump_json()
    await transport.add_message(manifest_message, send_before=1)

    async def add_job_messages(request_class, send_before=1, sleep_before=0, **kwargs):
        for job_uuid in job_uuids:
            msg = request_class(
                job_uuid=str(job_uuid),
                **kwargs,
            ).model_dump_json()
            await transport.add_message(msg, send_before=send_before, sleep_before=sleep_before)

    await add_job_messages(V0AcceptJobRequest, send_before=1, sleep_before=0.05)
    await add_job_messages(V0ExecutorReadyRequest, send_before=0)
    await add_job_messages(
        V0JobFinishedRequest,
        send_before=2,
        sleep_before=0.05,
        docker_process_stdout="",
        docker_process_stderr="",
        artifacts={},
    )

    batch = await SyntheticJobBatch.objects.acreate(
        block=1000,
        cycle=await Cycle.objects.acreate(start=708, stop=1430),
    )
    await asyncio.wait_for(
        execute_synthetic_batch_run(
            [miner],
            [],
            batch.id,
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=2,
    )

    await check_scores(job_uuids, transport)


async def check_scores(
    job_uuids: list[uuid.UUID],
    transport: SimulationTransport,
):
    job_finished_messages = transport.sent[-len(job_uuids) :]

    for msg in job_finished_messages:
        receipt = json.loads(msg)

        job_uuid = receipt["job_uuid"]

        job = await SyntheticJob.objects.aget(job_uuid=job_uuid)

        assert abs(job.score - 1) < 0.0001


@pytest.mark.override_config(DYNAMIC_NON_PEAK_CYCLE_EXECUTOR_MIN_RATIO=0.1)
async def test_synthetic_job_batch_non_peak_limits(
    job_generator_factory: TimeTookScoreMockSyntheticJobGeneratorFactory,
    miner: Miner,
    create_simulation_miner_client: Callable,
    transport: SimulationTransport,
    small_spin_up_times,
    override_weights_version_v2,
    settings,
):
    current_block = 10_000

    # setup peak
    peak_executor_count = 20
    peak_cycle_range = get_peak_cycle(current_block, settings.BITTENSOR_NETUID)
    assert current_block not in peak_cycle_range
    peak_cycle, _ = await Cycle.objects.aget_or_create(
        start=peak_cycle_range.start,
        stop=peak_cycle_range.stop,
    )
    miner, _ = await Miner.objects.aget_or_create(hotkey="miner_hotkey")
    peak_batch = await SyntheticJobBatch.objects.acreate(
        block=peak_cycle.start + 1,
        cycle=peak_cycle,
        scored=True,
        should_be_scored=True,
    )
    await MinerManifest.objects.acreate(
        miner=miner,
        batch=peak_batch,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        executor_count=peak_executor_count,
        online_executor_count=peak_executor_count,
    )

    executor_count = 10
    job_uuids = [uuid.uuid4() for _ in range(executor_count)]
    job_generator_factory._uuids = job_uuids.copy()

    manifest_message = V0ExecutorManifestRequest(
        manifest={DEFAULT_EXECUTOR_CLASS: executor_count}
    ).model_dump_json()
    await transport.add_message(manifest_message, send_before=1)

    async def add_job_messages(request_class, send_before=1, sleep_before=0, **kwargs):
        for i, job_uuid in enumerate(job_uuids):
            msg = request_class(
                job_uuid=str(job_uuid),
                **kwargs,
            ).model_dump_json()
            await transport.add_message(
                msg, send_before=send_before, sleep_before=sleep_before if i == 0 else 0
            )

    await add_job_messages(V0AcceptJobRequest, send_before=0, sleep_before=0.5)
    await add_job_messages(V0ExecutorReadyRequest, send_before=0)
    await add_job_messages(
        V0JobFinishedRequest,
        send_before=2,
        sleep_before=0.5,
        docker_process_stdout="",
        docker_process_stderr="",
    )

    batch = await SyntheticJobBatch.objects.acreate(
        block=current_block,
        cycle=await Cycle.objects.acreate(start=708, stop=1430),
    )
    await asyncio.wait_for(
        execute_synthetic_batch_run(
            [miner],
            [],
            batch.id,
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=2,
    )

    # check sent jobs were limited by ratio
    expected_executor_count = math.ceil(peak_executor_count * 0.1)

    def message_type_count(messages, type_) -> int:
        count = 0
        for message in messages:
            try:
                if json.loads(message).get("message_type") == type_:
                    count += 1
            except (AttributeError, json.JSONDecodeError):
                pass
        return count

    assert message_type_count(transport.sent, "V0InitialJobRequest") == expected_executor_count
    assert message_type_count(transport.sent, "V0JobRequest") == expected_executor_count


@pytest.mark.override_config(
    DYNAMIC_DEFAULT_EXECUTOR_LIMITS_FOR_MISSED_PEAK=f"{DEFAULT_EXECUTOR_CLASS}=1"
)
async def test_synthetic_job_batch_non_peak_limits__validator_missed_peak(
    job_generator_factory: TimeTookScoreMockSyntheticJobGeneratorFactory,
    miner: Miner,
    create_simulation_miner_client: Callable,
    transport: SimulationTransport,
    small_spin_up_times,
    override_weights_version_v2,
    settings,
):
    current_block = 10_000
    assert current_block not in get_peak_cycle(current_block, settings.BITTENSOR_NETUID)

    executor_count = 10
    job_uuids = [uuid.uuid4() for _ in range(executor_count)]
    job_generator_factory._uuids = job_uuids.copy()

    manifest_message = V0ExecutorManifestRequest(
        manifest={DEFAULT_EXECUTOR_CLASS: executor_count}
    ).model_dump_json()
    await transport.add_message(manifest_message, send_before=1)

    async def add_job_messages(request_class, send_before=1, sleep_before=0, **kwargs):
        for i, job_uuid in enumerate(job_uuids):
            msg = request_class(
                job_uuid=str(job_uuid),
                **kwargs,
            ).model_dump_json()
            await transport.add_message(
                msg, send_before=send_before, sleep_before=sleep_before if i == 0 else 0
            )

    await add_job_messages(V0AcceptJobRequest, send_before=0, sleep_before=0.5)
    await add_job_messages(V0ExecutorReadyRequest, send_before=0)
    await add_job_messages(
        V0JobFinishedRequest,
        send_before=2,
        sleep_before=0.5,
        docker_process_stdout="",
        docker_process_stderr="",
    )

    batch = await SyntheticJobBatch.objects.acreate(
        block=current_block,
        cycle=await Cycle.objects.acreate(start=708, stop=1430),
    )
    await asyncio.wait_for(
        execute_synthetic_batch_run(
            [miner],
            [],
            batch.id,
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=5,
    )

    # check sent jobs were limited by default limit
    expected_executor_count = 1

    def message_type_count(messages, type_) -> int:
        count = 0
        for message in messages:
            try:
                if json.loads(message).get("message_type") == type_:
                    count += 1
            except (AttributeError, json.JSONDecodeError):
                pass
        return count

    assert message_type_count(transport.sent, "V0InitialJobRequest") == expected_executor_count
    assert message_type_count(transport.sent, "V0JobRequest") == expected_executor_count


@pytest.mark.override_config(
    DYNAMIC_DEFAULT_EXECUTOR_LIMITS_FOR_MISSED_PEAK=f"{DEFAULT_EXECUTOR_CLASS}=1"
)
async def test_synthetic_job_batch_non_peak_limits__miner_missed_peak(
    job_generator_factory: TimeTookScoreMockSyntheticJobGeneratorFactory,
    miner: Miner,
    create_simulation_miner_client: Callable,
    transport: SimulationTransport,
    small_spin_up_times,
    override_weights_version_v2,
    settings,
):
    current_block = 10_000

    # setup peak
    peak_cycle_range = get_peak_cycle(current_block, settings.BITTENSOR_NETUID)
    assert current_block not in peak_cycle_range
    peak_cycle, _ = await Cycle.objects.aget_or_create(
        start=peak_cycle_range.start,
        stop=peak_cycle_range.stop,
    )
    miner, _ = await Miner.objects.aget_or_create(hotkey="miner_hotkey")
    _peak_batch = await SyntheticJobBatch.objects.acreate(
        block=peak_cycle.start + 1,
        cycle=peak_cycle,
        scored=True,
        should_be_scored=True,
    )

    executor_count = 10
    job_uuids = [uuid.uuid4() for _ in range(executor_count)]
    job_generator_factory._uuids = job_uuids.copy()

    manifest_message = V0ExecutorManifestRequest(
        manifest={DEFAULT_EXECUTOR_CLASS: executor_count}
    ).model_dump_json()
    await transport.add_message(manifest_message, send_before=1)

    async def add_job_messages(request_class, send_before=1, sleep_before=0, **kwargs):
        for i, job_uuid in enumerate(job_uuids):
            msg = request_class(
                job_uuid=str(job_uuid),
                **kwargs,
            ).model_dump_json()
            await transport.add_message(
                msg, send_before=send_before, sleep_before=sleep_before if i == 0 else 0
            )

    await add_job_messages(V0AcceptJobRequest, send_before=0, sleep_before=0.5)
    await add_job_messages(V0ExecutorReadyRequest, send_before=0)
    await add_job_messages(
        V0JobFinishedRequest,
        send_before=2,
        sleep_before=0.5,
        docker_process_stdout="",
        docker_process_stderr="",
    )

    batch = await SyntheticJobBatch.objects.acreate(
        block=current_block,
        cycle=await Cycle.objects.acreate(start=708, stop=1430),
    )
    await asyncio.wait_for(
        execute_synthetic_batch_run(
            [miner],
            [],
            batch.id,
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=5,
    )

    # check sent jobs were limited by default limit
    expected_executor_count = 1

    def message_type_count(messages, type_) -> int:
        count = 0
        for message in messages:
            try:
                if json.loads(message).get("message_type") == type_:
                    count += 1
            except (AttributeError, json.JSONDecodeError):
                pass
        return count

    assert message_type_count(transport.sent, "V0InitialJobRequest") == expected_executor_count
    assert message_type_count(transport.sent, "V0JobRequest") == expected_executor_count
