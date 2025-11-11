from unittest.mock import Mock, patch

import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_miner.miner.manifest_commitment import commit_manifest_to_subtensor


class TestCommitManifestToSubtensor:
    def test_commit_manifest_to_subtensor_success(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        wallet = Mock()
        wallet.hotkey.ss58_address = "test_hotkey"
        subtensor = Mock()

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
        subtensor.commit.assert_called_once_with(wallet, netuid, "<M:A3>")

    def test_commit_manifest_to_subtensor_failure(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        wallet = Mock()
        wallet.hotkey.ss58_address = "test_hotkey"
        subtensor = Mock()

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

        neuron = Mock()
        neuron.hotkey = "test_hotkey"
        neuron.uid = 5
        metagraph = Mock()
        metagraph.neurons = [neuron]
        subtensor.metagraph = Mock(return_value=metagraph)

        subtensor.get_commitment = Mock(return_value=None)
        subtensor.commit = Mock(side_effect=Exception("Network error"))
        netuid = 49

        with pytest.raises(Exception, match="Network error"):
            commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

    def test_commit_manifest_to_subtensor_empty_manifest(self):
        manifest = {}
        wallet = Mock()
        wallet.hotkey.ss58_address = "test_hotkey"
        subtensor = Mock()

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
        subtensor.commit.assert_called_once_with(wallet, netuid, "<M:>")

    def test_commit_manifest_to_subtensor_unchanged_manifest(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        wallet = Mock()
        wallet.hotkey.ss58_address = "test_hotkey"
        subtensor = Mock()

        neuron = Mock()
        neuron.hotkey = "test_hotkey"
        neuron.uid = 5
        metagraph = Mock()
        metagraph.neurons = [neuron]
        subtensor.metagraph = Mock(return_value=metagraph)

        subtensor.get_commitment = Mock(return_value="<M:A3>")
        subtensor.commit = Mock(return_value=True)
        netuid = 49

        result = commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is True
        subtensor.commit.assert_not_called()

    def test_commit_manifest_to_subtensor_unchanged_manifest_with_other_data(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        wallet = Mock()
        wallet.hotkey.ss58_address = "test_hotkey"
        subtensor = Mock()

        neuron = Mock()
        neuron.hotkey = "test_hotkey"
        neuron.uid = 5
        metagraph = Mock()
        metagraph.neurons = [neuron]
        subtensor.metagraph = Mock(return_value=metagraph)

        subtensor.get_commitment = Mock(return_value="https://example.com<M:A3>")
        subtensor.commit = Mock(return_value=True)
        netuid = 49

        result = commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is True
        subtensor.commit.assert_not_called()


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
        mock_subtensor_instance.get_commitment.return_value = "AG=2"
        mock_subtensor_instance.get_current_block.return_value = 120
        mock_bittensor.subtensor.return_value = mock_subtensor_instance

        mock_commit.return_value = True

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
        mock_subtensor_instance.get_commitment.return_value = "AG=3"
        mock_subtensor_instance.get_current_block.return_value = 220
        mock_bittensor.subtensor.return_value = mock_subtensor_instance

        mock_commit.return_value = True

        commit_manifest_to_chain()

        mock_commit.assert_called_once_with(
            manifest,
            mock_wallet,
            mock_subtensor_instance,
            49,
        )
