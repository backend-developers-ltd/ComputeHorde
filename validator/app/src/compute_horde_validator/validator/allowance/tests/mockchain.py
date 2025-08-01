import asyncio
import datetime
from contextlib import contextmanager
from functools import lru_cache
from unittest import mock
from unittest.mock import patch

import bittensor_wallet
import turbobt
from django.db import connections
from pydantic import BaseModel

from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.protocol_messages import V0ExecutorManifestRequest
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_validator.validator.allowance.utils import supertensor
from compute_horde_validator.validator.tests.transport import SimulationTransport

NUM_MINERS = 250
NUM_VALIDATORS = 6
NUM_BLOCKS = 100
START_BLOCK = 1000
START_CHANGING_MANIFESTS_BLOCK = 1010
START_CHANGING_STAKE_BLOCK = 1005
SYNC_MANIFESTS_INTERVAL = 25
MANIFEST_CHANGE_INTERVAL = 300
MANIFEST_FETCHING_TIMEOUT = 1.0

MINER_HOTKEYS = {
    **{i: f"stable_miner_{i:03d}" for i in range(NUM_MINERS // 2)},  # the manifests of these miners won't change
    **{i: f"whacky_miner_{i:03d}" for i in range(NUM_MINERS // 2, NUM_MINERS - 4)},
    # the manifests of these miners will be crazy, after block START_CHANGING_MANIFESTS_BLOCK
    NUM_MINERS - 5: f"always_increasing_miner_{NUM_MINERS - 5}",
    # the manifest of this miner will increase with each block after START_CHANGING_MANIFESTS_BLOCK
    NUM_MINERS - 4: f"forgetting_miner_{NUM_MINERS - 4}",
    # the manifest of this miner will be missing entries after START_CHANGING_MANIFESTS_BLOCK
    NUM_MINERS - 3: f"deregging_miner_{NUM_MINERS - 3}",
    # this miner will occasionally deregister after START_CHANGING_MANIFESTS_BLOCK
    NUM_MINERS - 2: f"timing_out_miner_{NUM_MINERS - 2}",
    # this miner will occasionally time out when sending the manifest after START_CHANGING_MANIFESTS_BLOCK
    NUM_MINERS - 1: f"malforming_miner_{NUM_MINERS - 1}",
    # this miner will occasionally send a malformed manifest after START_CHANGING_MANIFESTS_BLOCK
}


def cmbm(block_number):
    """
    Curated manifest block number
    """
    if block_number < START_CHANGING_MANIFESTS_BLOCK:
        return START_BLOCK
    return block_number

@lru_cache
def wallet():
    wallet_ = bittensor_wallet.Wallet(name="test_mock_validator")
    wallet_.regenerate_coldkey(
        mnemonic="local ghost evil lizard decade own lecture absurd vote despair predict cage",
        use_password=False,
        overwrite=True,
    )
    wallet_.regenerate_hotkey(
        mnemonic="position chicken ugly key sugar expect another require cinnamon rubber rich veteran",
        use_password=False,
        overwrite=True,
    )
    return wallet_


VALIDATOR_HOTKEYS = {
    **{i: f"regular_validator_{i}" for i in range(NUM_VALIDATORS - 2) if i != 2},
    2: wallet().get_hotkey().ss58_address,
    # these validators will have a steadily increasing stake
    NUM_VALIDATORS - 2: f"stake_loosing_validator_{NUM_VALIDATORS - 2}",
    # this validator will occasionally get a stake lower than 1000
    NUM_VALIDATORS - 1: f"deregging_validator_{NUM_VALIDATORS - 1}",  # this validator will occasionally deregister
}

EXECUTOR_CLASSES = [
    ExecutorClass.spin_up_4min__gpu_24gb,
    ExecutorClass.always_on__gpu_24gb,
    ExecutorClass.always_on__llm__a6000,
]

EXECUTOR_CAP = {
    ExecutorClass.spin_up_4min__gpu_24gb: 10,
    ExecutorClass.always_on__gpu_24gb: 5,
    ExecutorClass.always_on__llm__a6000: 3,
}


def manifest_responses(block_number) -> list[tuple[str, str | BaseModel, float]]:
    assert block_number >= START_BLOCK
    return [
        *[
            (
                hotkey,
                V0ExecutorManifestRequest(
                    job_uuid="nvm",
                    manifest={
                        ec: (ind + uid) % (EXECUTOR_CAP[ec]) for ind, ec in enumerate(EXECUTOR_CLASSES)
                    }
                ),
                0,
            )
            for uid, hotkey in MINER_HOTKEYS.items() if hotkey.startswith("stable_miner_")
        ],
        *[
            (
                hotkey,
                V0ExecutorManifestRequest(
                    job_uuid="nvm",
                    manifest={
                        ec: (ind + uid + cmbm(block_number)) % (EXECUTOR_CAP[ec]) for ind, ec in
                        enumerate(EXECUTOR_CLASSES)
                    }
                ),
                0,
            )
            for uid, hotkey in MINER_HOTKEYS.items() if hotkey.startswith("whacky_miner_")
        ],
        *[
            (
                hotkey,
                V0ExecutorManifestRequest(
                    job_uuid="nvm",
                    manifest={
                        ec: (ind + uid) % (EXECUTOR_CAP[ec]) + cmbm(block_number) - START_BLOCK for
                        ind, ec in
                        enumerate(EXECUTOR_CLASSES)
                    }
                ),
                0,
            )
            for uid, hotkey in MINER_HOTKEYS.items() if hotkey.startswith("always_increasing_miner_")
        ],
        *[
            (
                hotkey,
                V0ExecutorManifestRequest(
                    job_uuid="nvm",
                    manifest={
                        ec: (ind + uid + cmbm(block_number)) % (EXECUTOR_CAP[ec]) for
                        ind, ec in
                        enumerate(EXECUTOR_CLASSES) if
                        (block_number < START_CHANGING_MANIFESTS_BLOCK or (block_number + ind) % 2)
                    }
                ),
                0,
            )
            for uid, hotkey in MINER_HOTKEYS.items() if (
                    hotkey.startswith("forgetting_miner_") or
                    hotkey.startswith("deregging_miner_")
            )
        ],
        *[
            (
                hotkey,
                V0ExecutorManifestRequest(
                    job_uuid="nvm",
                    manifest={
                        ec: (ind + uid) % (EXECUTOR_CAP[ec]) for ind, ec in enumerate(EXECUTOR_CLASSES)
                    }
                ),
                0 if block_number < START_CHANGING_MANIFESTS_BLOCK else (MANIFEST_FETCHING_TIMEOUT * 2 * (block_number % 2)),
            )
            for uid, hotkey in MINER_HOTKEYS.items() if (
                hotkey.startswith("timing_out_miner_")
            )
        ],
        *[
            (
                hotkey,
                V0ExecutorManifestRequest(
                    job_uuid="nvm",
                    manifest={
                        ec: (ind + uid) % (EXECUTOR_CAP[ec]) for ind, ec in enumerate(EXECUTOR_CLASSES)
                    }
                ) if (block_number < START_CHANGING_MANIFESTS_BLOCK or not (block_number % 2)) else "wrong",
                0,
            )
            for uid, hotkey in MINER_HOTKEYS.items() if (
                hotkey.startswith("malforming_miner_")
            )
        ],

    ]


def _make_neuron(uid, key, stake, is_miner: bool, is_shielded) -> turbobt.Neuron:
    return turbobt.Neuron(
        subnet=mock.MagicMock(),
        uid=uid,
        coldkey=key,
        hotkey=key,
        active=True,
        axon_info=turbobt.neuron.AxonInfo(
            ip="0.0.0.0" if not is_miner else (f"192.168.1.{uid}" if not is_shielded else f"http://{key}.com"),
            port=8000 + uid if is_miner else 0,
            protocol=turbobt.neuron.AxonProtocolEnum.HTTP
        ),
        prometheus_info=mock.MagicMock(),
        stake=stake,
        rank=0,
        emission=0,
        incentive=0,
        consensus=0,
        trust=0,
        validator_trust=0,
        dividends=0,
        last_update=0,
        validator_permit=False,
        pruning_score=0,
    )


def stake(block_number: int, ind: int, hotkey: str) -> float:
    base_stake = 1001 * ((ind + 1) ** 2)

    if block_number < START_CHANGING_STAKE_BLOCK:
        return float(base_stake)

    blocks_elapsed = block_number - START_CHANGING_STAKE_BLOCK

    growth_multiplier = 1 + (blocks_elapsed * (0.001 + ind * 0.0002))

    stake = base_stake * growth_multiplier

    if hotkey.startswith("stake_loosing_validator_"):
        if not blocks_elapsed % 5:
            return 500.0
        else:
            return float(stake)

    return float(stake)


def list_validators(block_number: int, filter_=True) -> list[turbobt.Neuron]:
    assert block_number >= START_BLOCK
    return list(filter((lambda n: n.stake >= 1000) if filter_ else lambda n: True , [
        _make_neuron(
            ind + NUM_MINERS,
            hotkey,
            stake=stake(block_number, ind, hotkey),
            is_miner=False,
            is_shielded=False,

        ) for ind, hotkey in VALIDATOR_HOTKEYS.items() if (
            block_number < START_CHANGING_STAKE_BLOCK or
            not hotkey.startswith("deregging_validator_") or
            block_number % 6
        )
    ]))


def list_neurons(block_number: int, with_shield: bool) -> list[turbobt.Neuron]:
    assert block_number >= START_BLOCK
    return [
        *[
            _make_neuron(
                ind,
                hotkey,
                stake=float(ind),
                is_miner=True,
                is_shielded=bool(ind % 2) if with_shield else False,
            ) for ind, hotkey in MINER_HOTKEYS.items() if (
                block_number < START_CHANGING_MANIFESTS_BLOCK or
                not hotkey.startswith("deregging_miner_") or
                block_number % 5
            )
        ],
        *list_validators(block_number, filter_=False)
    ]


@contextmanager
def set_block_number(block_number_):
    class MockSuperTensor(supertensor.BaseSuperTensor):
        def get_current_block(self):
            return block_number_
        def get_shielded_neurons(self):
            return list_neurons(block_number_, with_shield=False)
        def list_neurons(self, block_number):
            return list_neurons(block_number, with_shield=False)
        def list_validators(self, block_number):
            return list_validators(block_number, filter_=True)
        def get_block_timestamp(self, block_number):
            base_time = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
            return base_time + datetime.timedelta(
                seconds=(block_number - START_BLOCK) * 12 + (0.01 if not (block_number % 5) else 0)
            )
        def wallet(self):
            return wallet()

    # Create transport map from manifest responses
    def create_transport_map():
        transport_map = {}
        responses = manifest_responses(block_number_)
        
        for hotkey, manifest_request, delay in responses:
            if isinstance(manifest_request, V0ExecutorManifestRequest):
                transport = SimulationTransport(f"sim_{hotkey}")
                # Use asyncio.run to handle the async operation
                asyncio.run(transport.add_message(manifest_request, send_before=1, sleep_before=delay))
                transport_map[hotkey] = transport
        
        return transport_map
    
    # Create mock init function for OrganicMinerClient
    def create_mock_init_function(transport_map):
        original_init = OrganicMinerClient.__init__
        
        def mock_init(self, miner_hotkey, miner_address, miner_port, job_uuid, my_keypair, transport=None):
            simulation_transport = transport_map.get(miner_hotkey)
            original_init(
                self,
                miner_hotkey,
                miner_address,
                miner_port,
                job_uuid,
                my_keypair,
                transport=simulation_transport,
            )
        
        return mock_init
    
    transport_map = create_transport_map()
    mock_init = create_mock_init_function(transport_map)

    with (
        patch.object(supertensor.supertensor, 'instance', MockSuperTensor()),
        patch.object(OrganicMinerClient, "__init__", mock_init),
    ):
        yield


# assert after manifests and one block
# next 5 blocks don't change in terms of stakes or manifests - check after them
# next 5 blocks change in terms of manifests - check after them
# next 5 blocks change in terms of manifests and stakes - check after them
# validators being deregged
# miners being deregged
# and then coming back again
# out of order blocks

# implement executor cap and patch the values in tests
# after generating 100 blocks - duplicate them back (remember about invalidations)
# find missing_blocks tests!
# assert after deregs
# give miners some minimal stake
# assert after drops regardless of order (block or manifest)
