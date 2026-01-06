import asyncio
import contextlib
import datetime as dt
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import pytest
from asgiref.sync import async_to_sync
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import V2JobRequest
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.protocol_messages import V0DeclineJobRequest
from compute_horde.transport import AbstractTransport
from compute_horde_core.executor_class import ExecutorClass as CoreExecutorClass
from django.conf import settings
from django.db.models import Sum as DjangoSum

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.types import Miner as AllowanceMiner
from compute_horde_validator.validator.allowance.types import ValidatorModel
from compute_horde_validator.validator.allowance.utils import blocks, manifests
from compute_horde_validator.validator.allowance.utils import supertensor as st_mod
from compute_horde_validator.validator.models import Miner, MinerIncident, OrganicJob
from compute_horde_validator.validator.models.allowance.internal import (
    AllowanceMinerManifest as _DbgAllowanceMinerManifest,
)
from compute_horde_validator.validator.models.allowance.internal import (
    Block as _DbgBlock,
)
from compute_horde_validator.validator.models.allowance.internal import (
    BlockAllowance as _DbgBlockAllowance,
)
from compute_horde_validator.validator.organic_jobs.facilitator_client import FacilitatorClient
from compute_horde_validator.validator.organic_jobs.miner_driver import drive_organic_job
from compute_horde_validator.validator.routing.default import routing
from compute_horde_validator.validator.routing.types import JobRoute, MinerIncidentType
from compute_horde_validator.validator.tests.transport import SimulationTransport


@dataclass
class MinerScenario:
    hotkey: str
    allowance: float
    executors: int
    incidents: int


JOB_REQUEST = V2JobRequest(
    uuid=str(uuid.uuid4()),
    executor_class=DEFAULT_EXECUTOR_CLASS,
    docker_image="doesntmatter",
    args=[],
    env={},
    download_time_limit=1,
    execution_time_limit=1,
    streaming_start_time_limit=1,
    upload_time_limit=1,
)


# Ensure collateral threshold defaults to 0 for these tests unless explicitly overridden
pytestmark = pytest.mark.override_config(DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI=0)


def reliability_env(
    *,
    monkeypatch,
    miners: list[MinerScenario],
    reservation_blocks: list[int],
    expected_blocks: Iterable[int] | None = None,
):
    """Run a reliability routing scenario using only public allowance pipeline functions.

    Previously this returned a factory; now it's a direct async helper invoked by tests.
    """

    class _FakeAxonInfo:
        def __init__(self, ip: str, port: int):
            self.ip = ip
            self.port = port

    class _FakeNeuron:
        def __init__(self, uid: int, hotkey: str, stake: float, port: int):
            self.uid = uid
            self.hotkey = hotkey
            self.coldkey = hotkey
            self.stake = stake
            self.axon_info = _FakeAxonInfo("127.0.0.1", port)

    def _mk_neuron(uid: int, hotkey: str, stake: float, port: int):
        return _FakeNeuron(uid, hotkey, stake, port)

    # Map miner hotkey -> port (must match what routing/_pick_miner_for_job_v2 will use)
    # routing builds miners from manifests/supertensor neuron list, where we set port=8000+idx
    port_by_hotkey = {m.hotkey: 8000 + idx for idx, m in enumerate(miners, start=1)}

    def _report_incidents(miner_hotkey: str, incidents: int, executor_class):
        """Simulate miner incidents.

        The production incident path is triggered from the organic job driver.
        After refactoring organic execution to be synchronous, calling the driver from
        async tests raises `SynchronousOnlyOperation`. For routing reliability tests, it
        is sufficient to record the corresponding `MinerIncident` rows.
        """
        expected_port = port_by_hotkey[miner_hotkey]
        miner_model, created = Miner.objects.get_or_create(
            hotkey=miner_hotkey,
            defaults={"address": "127.0.0.1", "port": expected_port, "ip_version": 4},
        )
        if not created and miner_model.port != expected_port:
            miner_model.port = expected_port
            miner_model.save(update_fields=["port"])

        for _ in range(incidents):
            routing().report_miner_incident(
                MinerIncidentType.MINER_JOB_REJECTED,
                hotkey_ss58address=miner_hotkey,
                job_uuid=str(uuid.uuid4()),
                executor_class=executor_class,
            )

    target_executor_class = CoreExecutorClass(JOB_REQUEST.executor_class)

    # Clean state for reliability scenario (avoid leakage from prior tests in worker)
    MinerIncident.objects.all().delete()
    _DbgBlockAllowance.objects.all().delete()
    _DbgAllowanceMinerManifest.objects.all().delete()
    _DbgBlock.objects.all().delete()

    base_block = reservation_blocks[0] - 1
    validator_hotkey = allowance().my_ss58_address
    neurons = [_mk_neuron(0, validator_hotkey, 10_000.0, 0)]
    for idx, m in enumerate(miners, start=1):
        neurons.append(_mk_neuron(idx, m.hotkey, 1_000.0, 8000 + idx))

    def _patch_fetch_manifests():
        async def _fake_fetch(_miners_arg):
            return {
                (m.hotkey, ec): (m.executors if ec == target_executor_class else 0)
                for m in miners
                for ec in CoreExecutorClass
            }

        monkeypatch.setattr(manifests, "fetch_manifests_from_miners", _fake_fetch, raising=False)
        monkeypatch.setattr(
            "compute_horde_validator.validator.allowance.utils.manifests.fetch_manifests_from_miners",
            _fake_fetch,
        )

    def _patch_supertensor(_st, base_block, neurons):
        monkeypatch.setattr(_st, "list_neurons", lambda block_number: neurons, raising=False)
        monkeypatch.setattr(_st, "get_shielded_neurons", lambda: neurons, raising=False)
        monkeypatch.setattr(
            _st,
            "get_block_timestamp",
            lambda bn, _bb=base_block: (
                dt.datetime(2024, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=12 * (bn - _bb))
            ),
            raising=False,
        )
        monkeypatch.setattr(
            _st,
            "list_validators",
            lambda bn: [
                ValidatorModel(
                    uid=neurons[0].uid, hotkey=neurons[0].hotkey, effective_stake=10000.0
                )
            ],
            raising=False,
        )
        monkeypatch.setattr(_st, "get_current_block", lambda: base_block, raising=False)
        monkeypatch.setattr(
            _st,
            "get_subnet_state",
            lambda bn: {"total_stake": [10000.0] + [1000.0] * len(miners)},
            raising=False,
        )

    def _advance_current_block(_st, bn):
        monkeypatch.setattr(_st, "get_current_block", lambda _bn=bn: _bn, raising=False)

    _patch_fetch_manifests()
    _st = st_mod.supertensor()
    _patch_supertensor(_st, base_block, neurons)

    manifests.sync_manifests()

    for bn in range(reservation_blocks[0], reservation_blocks[0] + 3):
        _advance_current_block(_st, bn)
        blocks.process_block_allowance_with_reporting(bn, supertensor_=_st)

    for m in miners:
        if m.incidents:
            _report_incidents(m.hotkey, m.incidents, target_executor_class)

    def _dbg_read():
        return list(
            _DbgBlockAllowance.objects.filter(
                miner_ss58__in=[m.hotkey for m in miners],
                executor_class=target_executor_class,
            )
            .values("miner_ss58")
            .annotate(total=DjangoSum("allowance"))
        )

    _allowance_totals = _dbg_read()
    assert _allowance_totals, "No BlockAllowance rows for scenario miners"
    for row in _allowance_totals:
        assert row["total"] > 0, (
            f"Zero allowance for {row['miner_ss58']} (allowance totals={_allowance_totals})"
        )

    job_route = routing().pick_miner_for_job_request(JOB_REQUEST)
    if expected_blocks is not None:
        assert job_route.allowance_blocks == list(expected_blocks)
    return job_route


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
@pytest.mark.parametrize(
    "miners,expected_winner,res_blocks,reason",
    [
        (
            [
                MinerScenario("rel_miner_a", allowance=100.0, executors=1, incidents=0),
                MinerScenario("rel_miner_b", allowance=100.0, executors=1, incidents=1),
            ],
            "rel_miner_a",
            [1001],
            "Fewer incidents wins",
        ),
        (
            [
                MinerScenario("rel_miner_exec_a", allowance=100.0, executors=1, incidents=1),
                MinerScenario("rel_miner_exec_b", allowance=100.0, executors=10, incidents=2),
            ],
            "rel_miner_exec_b",
            [1002],
            "Better per-executor reliability wins",
        ),
    ],
)
def test_reliability_sorting(
    miners: list[MinerScenario],
    expected_winner: str,
    res_blocks: list[int],
    reason: str,
    monkeypatch,
    disable_miner_shuffling,
):
    job_route = reliability_env(
        miners=miners,
        reservation_blocks=res_blocks,
        expected_blocks=res_blocks,
        monkeypatch=monkeypatch,
    )
    assert job_route.miner.hotkey_ss58 == expected_winner, reason
    assert job_route.allowance_blocks == res_blocks


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_excused_job_no_incident(monkeypatch):
    """If a miner rejects a job as BUSY but provides a valid excuse (sufficient receipts),
    the job is marked EXCUSED and no MinerIncident is recorded.

    This version simulates the full facilitator -> validator -> miner flow by pushing a
    job request through a facilitator SimulationTransport (using add_message) instead of
    pre-creating the OrganicJob directly.
    """

    job_uuid = str(uuid.uuid4())

    # Monkeypatch excuse helpers to simulate a valid excuse: 1 expected executor, 1 valid receipt
    def fake_filter_valid_excuse_receipts(**_kwargs):
        return [object()]

    def fake_get_expected_miner_executor_count(**_kwargs):
        return 1

    monkeypatch.setattr(
        "compute_horde_validator.validator.organic_jobs.miner_driver.job_excuses.filter_valid_excuse_receipts",
        fake_filter_valid_excuse_receipts,
    )
    monkeypatch.setattr(
        "compute_horde_validator.validator.organic_jobs.miner_driver.job_excuses.get_expected_miner_executor_count",
        fake_get_expected_miner_executor_count,
    )

    # Facilitator transport (auth success + job request)
    faci_transport = SimulationTransport("facilitator_excused_case")
    async_to_sync(faci_transport.add_message)(
        '{"status":"success"}', send_before=1
    )  # auth response

    request = V2JobRequest(
        uuid=job_uuid,
        executor_class=CoreExecutorClass.always_on__gpu_24gb,
        docker_image="ubuntu:latest",
        args=[],
        env={},
        download_time_limit=1,
        execution_time_limit=1,
        streaming_start_time_limit=1,
        upload_time_limit=1,
    )
    async_to_sync(faci_transport.add_message)(request, send_before=0)

    # Patch routing to return a deterministic miner; we bypass full allowance + miner driver.

    def fake_pick_miner(request):  # noqa: D401
        # Ensure Miner ORM row so later code linking incidents / status updates works
        orm_miner, _ = Miner.objects.get_or_create(
            hotkey="excused_miner_hotkey",
            defaults={"address": "127.0.0.1", "port": 4321, "ip_version": 4},
        )
        return JobRoute(
            miner=AllowanceMiner(
                address=orm_miner.address or "127.0.0.1",
                ip_version=orm_miner.ip_version or 4,
                port=orm_miner.port or 4321,
                hotkey_ss58=orm_miner.hotkey,
            ),
            allowance_blocks=[],
            allowance_reservation_id=1,
            allowance_job_value=0,
        )

    class _FakeRouting:
        def pick_miner_for_job_request(self, request):  # noqa: D401
            return fake_pick_miner(request)

    monkeypatch.setattr(
        "compute_horde_validator.validator.organic_jobs.facilitator_client.routing",  # where it's imported
        lambda: _FakeRouting(),
    )

    class _SimulationTransportWsAdapter:
        def __init__(self, transport: AbstractTransport):
            self.transport = transport

        async def send(self, msg: str):  # noqa: D401
            await self.transport.send(msg)

        async def recv(self) -> str:  # noqa: D401
            data = await self.transport.receive()
            if isinstance(data, (bytes, bytearray, memoryview)):  # noqa: UP038
                return bytes(data).decode()
            return str(data)

        def __aiter__(self):
            return self.transport

    # Patch job execution to directly create an EXCUSED OrganicJob without invoking miner driver / celery
    async def fake_execute(job_request, job_route):  # noqa: D401
        orm_miner = await Miner.objects.aget(hotkey="excused_miner_hotkey")
        job = await OrganicJob.objects.acreate(
            job_uuid=job_request.uuid,
            miner=orm_miner,
            miner_address=orm_miner.address or "127.0.0.1",
            miner_address_ip_version=orm_miner.ip_version or 4,
            miner_port=orm_miner.port or 4321,
            executor_class=job_request.executor_class.value,
            job_description="excused test job",
            block=0,
            status=OrganicJob.Status.EXCUSED,
        )
        return job

    with (
        patch.object(FacilitatorClient, "heartbeat", AsyncMock()),
        patch.object(FacilitatorClient, "wait_for_specs", AsyncMock()),
        patch(
            "compute_horde_validator.validator.organic_jobs.facilitator_client.verify_request_or_fail",
            AsyncMock(),
        ),
        patch(
            "compute_horde_validator.validator.organic_jobs.facilitator_client.execute_organic_job_request_on_worker",
            fake_execute,
        ),
    ):
        faci_client = FacilitatorClient(settings.BITTENSOR_WALLET().hotkey, "")

        async def run_until():
            async with faci_client, asyncio.timeout(2):
                task = asyncio.create_task(
                    faci_client.handle_connection(_SimulationTransportWsAdapter(faci_transport))
                )
                # Wait until job recorded as EXCUSED
                while True:
                    try:
                        job_obj = await OrganicJob.objects.aget(job_uuid=job_uuid)
                        if job_obj.status == OrganicJob.Status.EXCUSED:
                            break
                    except OrganicJob.DoesNotExist:  # noqa: PERF203
                        pass
                    await asyncio.sleep(0.05)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        async_to_sync(run_until)()

    job = OrganicJob.objects.get(job_uuid=job_uuid)
    assert job.status == OrganicJob.Status.EXCUSED
    assert MinerIncident.objects.count() == 0
