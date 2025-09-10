import datetime as dt
import uuid
from collections.abc import Iterable
from dataclasses import dataclass

import pytest
from asgiref.sync import sync_to_async
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import V2JobRequest
from compute_horde_core.executor_class import ExecutorClass as CoreExecutorClass
from django.db.models import Sum as DjangoSum

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.utils import blocks, manifests
from compute_horde_validator.validator.allowance.utils import supertensor as st_mod
from compute_horde_validator.validator.models import MinerIncident
from compute_horde_validator.validator.models.allowance.internal import (
    AllowanceMinerManifest as _DbgAllowanceMinerManifest,
)
from compute_horde_validator.validator.models.allowance.internal import (
    Block as _DbgBlock,
)
from compute_horde_validator.validator.models.allowance.internal import (
    BlockAllowance as _DbgBlockAllowance,
)
from compute_horde_validator.validator.routing.default import routing
from compute_horde_validator.validator.routing.types import MinerIncidentType


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
    use_gpu=False,
    download_time_limit=1,
    execution_time_limit=1,
    streaming_start_time_limit=1,
    upload_time_limit=1,
)


async def reliability_env(
    *,
    monkeypatch,
    miners: list[MinerScenario],
    reservation_blocks: list[int],
    expected_blocks: Iterable[int] | None = None,
):
    """Run a reliability routing scenario using only public allowance pipeline functions.

    Previously this returned a factory; now it's a direct async helper invoked by tests.
    """

    # fetch_manifests_from_miners imported indirectly via manifests module at test top

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

    async def _report_incidents(miner: str, incidents: int, executor_class):
        for _ in range(incidents):
            await routing().report_miner_incident(
                type=MinerIncidentType.MINER_JOB_REJECTED,
                hotkey_ss58address=miner,
                job_uuid=str(uuid.uuid4()),
                executor_class=executor_class,
            )

    target_executor_class = CoreExecutorClass(JOB_REQUEST.executor_class)

    # Clean state for reliability scenario (avoid leakage from prior tests in worker)
    await MinerIncident.objects.all().adelete()
    await _DbgBlockAllowance.objects.all().adelete()
    await _DbgAllowanceMinerManifest.objects.all().adelete()
    await _DbgBlock.objects.all().adelete()

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
        if hasattr(_st, "_neuron_list_cache"):
            try:
                _st._neuron_list_cache.clear()  # type: ignore[attr-defined]
            except Exception:
                pass
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
        monkeypatch.setattr(_st, "list_validators", lambda bn: [neurons[0]], raising=False)
        monkeypatch.setattr(_st, "get_current_block", lambda: base_block, raising=False)

    def _advance_current_block(_st, bn):
        monkeypatch.setattr(_st, "get_current_block", lambda _bn=bn: _bn, raising=False)

    _patch_fetch_manifests()
    _st = st_mod.supertensor()
    _patch_supertensor(_st, base_block, neurons)

    await sync_to_async(manifests.sync_manifests)()

    for bn in range(reservation_blocks[0], reservation_blocks[0] + 3):
        _advance_current_block(_st, bn)
        await sync_to_async(blocks.process_block_allowance_with_reporting)(bn, supertensor_=_st)

    for m in miners:
        if m.incidents:
            await _report_incidents(m.hotkey, m.incidents, target_executor_class)

    def _dbg_read():
        return list(
            _DbgBlockAllowance.objects.filter(
                miner_ss58__in=[m.hotkey for m in miners],
                executor_class=target_executor_class,
            )
            .values("miner_ss58")
            .annotate(total=DjangoSum("allowance"))
        )

    _allowance_totals = await sync_to_async(_dbg_read)()
    assert _allowance_totals, "No BlockAllowance rows for scenario miners"
    for row in _allowance_totals:
        assert row["total"] > 0, (
            f"Zero allowance for {row['miner_ss58']} (allowance totals={_allowance_totals})"
        )

    job_route = await routing().pick_miner_for_job_request(JOB_REQUEST)
    if expected_blocks is not None:
        assert job_route.allowance_blocks == list(expected_blocks)
    return job_route


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
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
        (
            [
                MinerScenario("rel_miner_tie_a", allowance=50.0, executors=3, incidents=1),
                MinerScenario("rel_miner_tie_b", allowance=60.0, executors=3, incidents=1),
            ],
            "rel_miner_tie_a",
            [1003],
            "Tie on reliability => original allowance order kept",
        ),
    ],
)
async def test_reliability_sorting(
    miners: list[MinerScenario],
    expected_winner: str,
    res_blocks: list[int],
    reason: str,
    monkeypatch,
):
    job_route = await reliability_env(
        miners=miners,
        reservation_blocks=res_blocks,
        expected_blocks=res_blocks,
        monkeypatch=monkeypatch,
    )
    assert job_route.miner.hotkey_ss58 == expected_winner, reason
    assert job_route.allowance_blocks == res_blocks
