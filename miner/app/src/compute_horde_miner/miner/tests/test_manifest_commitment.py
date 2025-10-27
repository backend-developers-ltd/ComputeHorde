from unittest.mock import Mock, patch

import pytest
from compute_horde.manifest_utils import format_manifest_commitment
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_miner.miner.manifest_commitment import (
    commit_manifest_to_subtensor,
    extract_manifest_payload,
    merge_manifest,
    parse_commitment_string,
)


class TestFormatManifestCommitment:
    def test_format_manifest_commitment(self):
        manifest = {
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
            ExecutorClass.always_on__gpu_24gb: 3,
        }
        result = format_manifest_commitment(manifest)
        assert result == "always_on.gpu-24gb=3;spin_up-4min.gpu-24gb=2"

    def test_format_manifest_commitment_empty(self):
        manifest = {}
        result = format_manifest_commitment(manifest)
        assert result == ""

    def test_format_manifest_commitment_single_class(self):
        manifest = {ExecutorClass.always_on__llm__a6000: 5}
        result = format_manifest_commitment(manifest)
        assert result == "always_on.llm.a6000=5"

    def test_format_manifest_commitment_all_classes(self):
        manifest = {
            ExecutorClass.spin_up_4min__gpu_24gb: 1,
            ExecutorClass.always_on__gpu_24gb: 2,
            ExecutorClass.always_on__llm__a6000: 3,
            ExecutorClass.always_on__test: 5,
        }
        result = format_manifest_commitment(manifest)
        expected = (
            "always_on.gpu-24gb=2;always_on.llm.a6000=3;always_on.test=5;spin_up-4min.gpu-24gb=1"
        )
        assert result == expected

    def test_format_manifest_commitment_too_long(self):
        long_manifest = {
            ExecutorClass.always_on__gpu_24gb: 999999999999999999999999999999999999999999999999,
            ExecutorClass.always_on__llm__a6000: 999999999999999999999999999999999999999999999999,
            ExecutorClass.spin_up_4min__gpu_24gb: 999999999999999999999999999999999999999999999999,
        }
        with pytest.raises(ValueError) as exc_info:
            format_manifest_commitment(long_manifest)
        assert "exceeds maximum" in str(exc_info.value)


class TestParseCommitmentString:
    def test_parse_commitment_string(self):
        commitment = "always_on.gpu-24gb=3;spin_up-4min.gpu-24gb=2"
        result = parse_commitment_string(commitment)
        expected = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
        }
        assert result == expected

    def test_parse_commitment_string_empty(self):
        result = parse_commitment_string("")
        assert result == {}

    def test_parse_commitment_string_invalid_format(self):
        # Missing equals sign
        result = parse_commitment_string("always_on.gpu-24gb:3")
        assert result == {}

    def test_parse_commitment_string_invalid_executor_class(self):
        # Invalid executor class name
        result = parse_commitment_string("invalid-executor-class=6")
        assert result == {}

    def test_parse_commitment_string_invalid_count(self):
        result = parse_commitment_string("always_on.gpu-24gb=abc")
        assert result == {}

    def test_parse_commitment_string_with_spaces(self):
        commitment = " always_on.gpu-24gb = 3 ; spin_up-4min.gpu-24gb = 2 "
        result = parse_commitment_string(commitment)
        expected = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
        }
        assert result == expected

    def test_parse_commitment_string_partial_invalid(self):
        # One valid, one invalid
        commitment = "always_on.gpu-24gb=3;invalid-class=2"
        result = parse_commitment_string(commitment)
        expected = {ExecutorClass.always_on__gpu_24gb: 3}
        assert result == expected


class TestEnvelopeHandling:
    """Tests for manifest envelope extraction and merging."""

    def test_extract_manifest_payload_with_envelope(self):
        commitment = "https://example.com/data<<CHM:always_on.gpu-24gb=2>> extra"
        other_data, payload = extract_manifest_payload(commitment)
        assert payload == "always_on.gpu-24gb=2"
        assert "https://example.com/data" in other_data
        assert "extra" in other_data

    def test_extract_manifest_payload_empty_envelope(self):
        commitment = "<<CHM:>>"
        other_data, payload = extract_manifest_payload(commitment)
        assert payload == ""
        assert other_data == ""

    def test_merge_manifest_empty_commitment(self):
        result = merge_manifest("", "always_on.gpu-24gb=2")
        assert result == "<<CHM:always_on.gpu-24gb=2>>"

    def test_merge_manifest_no_existing_envelope(self):
        existing = "https://example.com/data"
        result = merge_manifest(existing, "always_on.gpu-24gb=3")
        assert result == "https://example.com/data<<CHM:always_on.gpu-24gb=3>>"

    def test_merge_manifest_replaces_existing_envelope(self):
        existing = "https://example.com/data<<CHM:always_on.gpu-24gb=2>>"
        result = merge_manifest(existing, "always_on.gpu-24gb=3")
        assert result == "https://example.com/data<<CHM:always_on.gpu-24gb=3>>"

    def test_merge_manifest_preserves_other_data(self):
        existing = "start<<CHM:old=1>> end"
        result = merge_manifest(existing, "new=2")
        assert "start" in result
        assert "end" in result
        assert "<<CHM:new=2>>" in result
        assert "old=1" not in result

    def test_merge_manifest_empty_manifest_string(self):
        existing = "https://example.com/data<<CHM:always_on.gpu-24gb=2>>"
        result = merge_manifest(existing, "")
        assert result == "https://example.com/data<<CHM:>>"

    def test_merge_manifest_round_trip(self):
        original = '{"contract": "0xabc"}<<CHM:always_on.gpu-24gb=5>>'
        other_data, payload = extract_manifest_payload(original)

        merged = merge_manifest(original, "always_on.gpu-24gb=7")

        new_other_data, new_payload = extract_manifest_payload(merged)
        assert new_other_data == other_data
        assert new_payload == "always_on.gpu-24gb=7"


class TestCommitManifestToSubtensor:
    def test_commit_manifest_to_subtensor_success(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        wallet = Mock()
        wallet.hotkey.ss58_address = "test_hotkey"
        subtensor = Mock()

        # Mock metagraph to find uid
        neuron = Mock()
        neuron.hotkey = "test_hotkey"
        neuron.uid = 5
        metagraph = Mock()
        metagraph.neurons = [neuron]
        subtensor.metagraph = Mock(return_value=metagraph)

        # No existing commitment
        subtensor.get_commitment = Mock(return_value=None)
        subtensor.commit = Mock(return_value=True)
        netuid = 49

        result = commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is True
        # Should commit with envelope since no existing commitment
        subtensor.commit.assert_called_once_with(wallet, netuid, "<<CHM:always_on.gpu-24gb=3>>")

    def test_commit_manifest_to_subtensor_failure(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        wallet = Mock()
        wallet.hotkey.ss58_address = "test_hotkey"
        subtensor = Mock()

        # Mock metagraph to find uid
        neuron = Mock()
        neuron.hotkey = "test_hotkey"
        neuron.uid = 5
        metagraph = Mock()
        metagraph.neurons = [neuron]
        subtensor.metagraph = Mock(return_value=metagraph)

        subtensor.get_commitment = Mock(return_value=None)
        subtensor.commit = Mock(return_value=False)
        netuid = 49

        result = commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is False

    def test_commit_manifest_to_subtensor_exception(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        wallet = Mock()
        wallet.hotkey.ss58_address = "test_hotkey"
        subtensor = Mock()

        # Mock metagraph to find uid
        neuron = Mock()
        neuron.hotkey = "test_hotkey"
        neuron.uid = 5
        metagraph = Mock()
        metagraph.neurons = [neuron]
        subtensor.metagraph = Mock(return_value=metagraph)

        subtensor.get_commitment = Mock(return_value=None)
        subtensor.commit = Mock(side_effect=Exception("Network error"))
        netuid = 49

        result = commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is False

    def test_commit_manifest_to_subtensor_empty_manifest(self):
        manifest = {}
        wallet = Mock()
        wallet.hotkey.ss58_address = "test_hotkey"
        subtensor = Mock()

        # Mock metagraph to find uid
        neuron = Mock()
        neuron.hotkey = "test_hotkey"
        neuron.uid = 5
        metagraph = Mock()
        metagraph.neurons = [neuron]
        subtensor.metagraph = Mock(return_value=metagraph)

        subtensor.get_commitment = Mock(return_value=None)
        subtensor.commit = Mock(return_value=True)
        netuid = 49

        result = commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is True
        subtensor.commit.assert_called_once_with(wallet, netuid, "<<CHM:>>")


class TestRoundTrip:
    def test_round_trip_multiple_classes(self):
        original = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
            ExecutorClass.always_on__llm__a6000: 1,
        }
        commitment = format_manifest_commitment(original)
        parsed = parse_commitment_string(commitment)
        assert original == parsed


@pytest.mark.django_db
class TestCommitManifestToChainTask:
    @patch("compute_horde_miner.miner.tasks.commit_manifest_to_subtensor")
    @patch("compute_horde_miner.miner.tasks.async_to_sync")
    @patch("compute_horde_miner.miner.tasks.current")
    @patch("compute_horde_miner.miner.tasks.settings")
    @patch("compute_horde_miner.miner.tasks.bittensor")
    def test_commit_manifest_to_chain_task_manifest_changed(
        self,
        mock_bittensor,
        mock_settings,
        mock_current,
        mock_async_to_sync,
        mock_commit,
    ):
        from compute_horde_miner.miner.tasks import commit_manifest_to_chain

        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_async_to_sync.side_effect = lambda f: lambda: manifest

        mock_wallet = Mock()
        mock_wallet.hotkey.ss58_address = "test_hotkey"
        mock_settings.BITTENSOR_WALLET.return_value = mock_wallet
        mock_settings.BITTENSOR_NETWORK = "test"
        mock_settings.BITTENSOR_NETUID = 49

        mock_subtensor_instance = Mock()
        mock_metagraph = Mock()
        mock_metagraph.hotkeys = ["other_hotkey", "test_hotkey"]
        mock_subtensor_instance.metagraph.return_value = mock_metagraph
        mock_subtensor_instance.get_commitment.return_value = "always_on.gpu-24gb=2"
        mock_subtensor_instance.get_current_block.return_value = 120
        mock_bittensor.subtensor.return_value = mock_subtensor_instance

        mock_commit.return_value = True

        # Run task
        commit_manifest_to_chain()

        mock_commit.assert_called_once_with(
            manifest,
            mock_wallet,
            mock_subtensor_instance,
            49,
        )

    @patch("compute_horde_miner.miner.tasks.commit_manifest_to_subtensor")
    @patch("compute_horde_miner.miner.tasks.async_to_sync")
    @patch("compute_horde_miner.miner.tasks.current")
    @patch("compute_horde_miner.miner.tasks.settings")
    @patch("compute_horde_miner.miner.tasks.bittensor")
    def test_commit_manifest_to_chain_task_empty_manifest(
        self, mock_bittensor, mock_settings, mock_current, mock_async_to_sync, mock_commit
    ):
        from compute_horde_miner.miner.tasks import commit_manifest_to_chain

        # Empty manifest should be allowed (for pausing)
        manifest = {}
        mock_async_to_sync.side_effect = lambda f: lambda: manifest

        mock_wallet = Mock()
        mock_wallet.hotkey.ss58_address = "test_hotkey"
        mock_settings.BITTENSOR_WALLET.return_value = mock_wallet
        mock_settings.BITTENSOR_NETWORK = "test"
        mock_settings.BITTENSOR_NETUID = 49

        mock_subtensor_instance = Mock()
        mock_metagraph = Mock()
        mock_metagraph.hotkeys = ["another_hotkey", "test_hotkey"]
        mock_subtensor_instance.metagraph.return_value = mock_metagraph
        # Chain has a non-empty manifest, we want to commit empty
        mock_subtensor_instance.get_commitment.return_value = "always_on.gpu-24gb=3"
        mock_subtensor_instance.get_current_block.return_value = 220
        mock_bittensor.subtensor.return_value = mock_subtensor_instance

        mock_commit.return_value = True

        # Run task - should commit empty manifest
        commit_manifest_to_chain()

        mock_commit.assert_called_once_with(
            manifest,
            mock_wallet,
            mock_subtensor_instance,
            49,
        )
