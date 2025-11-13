from unittest.mock import Mock, patch

import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.allowance.utils.manifests import (
    get_current_manifests,
    parse_commitments_to_manifests,
)


class TestParseCommitmentsToManifests:
    def test_parse_commitments(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"<M:A3;S2>",
            "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty": b"<M:L1>",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 2
        assert result["5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"] == {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
        }
        assert result["5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"] == {
            ExecutorClass.always_on__llm__a6000: 1,
        }

    def test_parse_commitments_with_other_data(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"https://example.com<M:A5>",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 1
        assert result["5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"] == {
            ExecutorClass.always_on__gpu_24gb: 5,
        }

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
        assert result["5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"] == {
            ExecutorClass.always_on__gpu_24gb: 3,
        }

    def test_parse_commitments_empty_dict(self):
        commitments: dict[str, bytes] = {}

        result = parse_commitments_to_manifests(commitments)

        assert result == {}


class TestGetCurrentManifestsKnowledgeCommitments:
    @pytest.mark.django_db
    @pytest.mark.override_config(DYNAMIC_MANIFESTS_USE_KNOWLEDGE_COMMITMENTS=True)
    @patch("compute_horde_validator.validator.allowance.utils.manifests.supertensor")
    def test_knowledge_commitments_mode_with_data(self, mock_supertensor):
        mock_st = Mock()
        mock_st.get_current_block.return_value = 1000
        mock_st.get_commitments.return_value = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"<M:A3;S2>",
        }
        mock_supertensor.return_value = mock_st

        result = get_current_manifests()

        assert len(result) == 1
        assert result["5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"] == {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
        }
        mock_st.get_current_block.assert_called_once()
        mock_st.get_commitments.assert_called_once_with(1000)

    @pytest.mark.django_db
    @pytest.mark.override_config(DYNAMIC_MANIFESTS_USE_KNOWLEDGE_COMMITMENTS=True)
    @patch("compute_horde_validator.validator.allowance.utils.manifests.supertensor")
    def test_knowledge_commitments_mode_with_none(self, mock_supertensor):
        mock_st = Mock()
        mock_st.get_current_block.return_value = 1000
        mock_st.get_commitments.return_value = None
        mock_supertensor.return_value = mock_st

        result = get_current_manifests()

        assert result == {}
        mock_st.get_current_block.assert_called_once()
        mock_st.get_commitments.assert_called_once_with(1000)
