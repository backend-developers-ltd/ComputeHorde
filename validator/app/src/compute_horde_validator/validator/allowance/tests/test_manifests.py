from unittest.mock import Mock, patch

import pytest
from compute_horde_core.executor_class import ExecutorClass
from pylon_client.artanis.unstable import HexDataCommitment

from compute_horde_validator.validator.allowance.tests.mockchain import (
    MINER_HOTKEYS,
    set_block_number,
)
from compute_horde_validator.validator.allowance.utils.manifests import (
    get_manifests,
    parse_commitments_to_manifests,
    sync_manifests,
)
from compute_horde_validator.validator.allowance.utils.supertensor import SuperTensor, supertensor
from compute_horde_validator.validator.tests.helpers import patch_constance


class TestParseCommitmentsToManifests:
    def test_parse_commitments(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"<M:A3;S2>",
            "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty": b"<M:L1>",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 3
        assert (
            result[
                (
                    "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                    ExecutorClass.always_on__gpu_24gb,
                )
            ]
            == 3
        )
        assert (
            result[
                (
                    "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                    ExecutorClass.spin_up_4min__gpu_24gb,
                )
            ]
            == 2
        )
        assert (
            result[
                (
                    "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
                    ExecutorClass.always_on__llm__a6000,
                )
            ]
            == 1
        )

    def test_parse_commitments_with_other_data(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"https://example.com<M:A5>",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 1
        assert (
            result[
                (
                    "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                    ExecutorClass.always_on__gpu_24gb,
                )
            ]
            == 5
        )

    def test_parse_commitments_empty_envelope(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"<M:>",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 0

    def test_parse_commitments_no_envelope(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"https://example.com",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 0

    def test_parse_commitments_mixed_valid_invalid(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"<M:A3>",
            "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty": b"invalid data",
            "5DAAnrj7VHTznn2AWBemMuyBwZWs6FNFjdyVXUeYum3PTXFy": b"\xff\xfe",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 1
        assert (
            result[
                (
                    "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                    ExecutorClass.always_on__gpu_24gb,
                )
            ]
            == 3
        )

    def test_parse_commitments_empty_dict(self):
        commitments: dict[str, bytes] = {}

        result = parse_commitments_to_manifests(commitments)

        assert result == {}

    def test_parse_commitments_beside_factory_horde(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": (
                b"<D:https://example.com/s><F:factory-horde-factory@" + b"A" * 43 + b"><M:A5>"
            ),
        }

        result = parse_commitments_to_manifests(commitments)

        assert result == {
            (
                "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                ExecutorClass.always_on__gpu_24gb,
            ): 5
        }


@pytest.mark.django_db(transaction=True, databases=["default", "default_alias"])
def test_expiring_chain_manifest(settings):
    settings.MANIFEST_COMMITMENT_MAX_AGE_BLOCKS = 10
    miner = MINER_HOTKEYS[0]
    commitment = HexDataCommitment.model_construct(
        hotkey=miner, commitment_block_number=990, commitment="0x" + b"<M:A3;S2>".hex()
    )
    with (
        set_block_number(1000),
        patch.object(supertensor(), "get_current_block", side_effect=[1000, 1001]),
        patch_constance({"DYNAMIC_MANIFESTS_USE_KNOWLEDGE_COMMITMENTS": True}),
        patch.object(
            type(supertensor()), "get_recent_commitments", SuperTensor.get_recent_commitments
        ),
        patch(
            "compute_horde_validator.validator.allowance.utils.supertensor.pylon_client"
        ) as client,
    ):
        identity = client.return_value.__enter__.return_value.unstable.identity
        identity.get_commitments.return_value = Mock(
            block=Mock(number=1000), commitments={miner: commitment}
        )
        sync_manifests()
        zeros = {(miner, cls): 0 for cls in ExecutorClass}
        assert get_manifests(1000, [miner]) == {
            **zeros,
            (miner, ExecutorClass.always_on__gpu_24gb): 3,
            (miner, ExecutorClass.spin_up_4min__gpu_24gb): 2,
        }

        sync_manifests()
        assert get_manifests(1001, [miner]) == zeros
