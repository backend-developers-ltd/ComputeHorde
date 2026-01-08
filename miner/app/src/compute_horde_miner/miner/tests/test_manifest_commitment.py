from unittest.mock import Mock, patch

import pytest
from compute_horde_core.executor_class import ExecutorClass
from pylon_client.v1 import PylonRequestException, PylonResponseException

from compute_horde_miner.miner.manifest_commitment import commit_manifest


class TestCommitManifest:
    def test_commit_manifest_success(self, mock_pylon_client):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_pylon_client.identity.get_own_commitment.return_value = Mock(commitment=None)

        result = commit_manifest(manifest, mock_pylon_client)

        assert result is True
        mock_pylon_client.identity.set_commitment.assert_called_once_with(b"<M:A3>")

    def test_commit_manifest_failure(self, mock_pylon_client):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_pylon_client.identity.get_own_commitment.return_value = Mock(commitment=None)
        mock_pylon_client.identity.set_commitment.side_effect = PylonResponseException()

        result = commit_manifest(manifest, mock_pylon_client)

        assert result is False

    def test_commit_manifest_exception_propagates(self, mock_pylon_client):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_pylon_client.identity.get_own_commitment.return_value = Mock(commitment=None)
        mock_pylon_client.identity.set_commitment.side_effect = PylonRequestException()

        with pytest.raises(PylonRequestException):
            commit_manifest(manifest, mock_pylon_client)

    def test_commit_manifest_empty_manifest(self, mock_pylon_client):
        manifest = {}
        mock_pylon_client.identity.get_own_commitment.return_value = Mock(commitment=None)

        result = commit_manifest(manifest, mock_pylon_client)

        assert result is True
        mock_pylon_client.identity.set_commitment.assert_called_once_with(b"<M:>")

    def test_commit_manifest_unchanged_manifest(self, mock_pylon_client):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_pylon_client.identity.get_own_commitment.return_value = Mock(commitment="<M:A3>")

        result = commit_manifest(manifest, mock_pylon_client)

        assert result is True
        mock_pylon_client.identity.set_commitment.assert_not_called()

    def test_commit_manifest_unchanged_manifest_with_other_data(self, mock_pylon_client):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_pylon_client.identity.get_own_commitment.return_value = Mock(
            commitment="https://example.com<M:A3>"
        )

        result = commit_manifest(manifest, mock_pylon_client)

        assert result is True
        mock_pylon_client.identity.set_commitment.assert_not_called()


@pytest.mark.django_db
class TestCommitManifestToChainTask:
    @patch("compute_horde_miner.miner.tasks.commit_manifest")
    @patch("compute_horde_miner.miner.tasks.pylon_client")
    @patch("compute_horde_miner.miner.tasks.async_to_sync")
    @patch("compute_horde_miner.miner.executor_manager.current.executor_manager")
    def test_commit_manifest_to_chain_task_manifest_changed(
        self,
        mock_executor_manager,
        mock_async_to_sync,
        mock_pylon_client,
        mock_commit,
    ):
        from compute_horde_miner.miner.tasks import commit_manifest_to_chain

        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_async_to_sync.side_effect = lambda f: lambda: manifest
        mock_commit.return_value = True

        commit_manifest_to_chain()

        mock_commit.assert_called_once_with(
            manifest, mock_pylon_client.return_value.__enter__.return_value
        )

    @patch("compute_horde_miner.miner.tasks.commit_manifest")
    @patch("compute_horde_miner.miner.tasks.pylon_client")
    @patch("compute_horde_miner.miner.tasks.async_to_sync")
    @patch("compute_horde_miner.miner.executor_manager.current.executor_manager")
    def test_commit_manifest_to_chain_task_empty_manifest(
        self,
        mock_executor_manager,
        mock_async_to_sync,
        mock_pylon_client,
        mock_commit,
    ):
        from compute_horde_miner.miner.tasks import commit_manifest_to_chain

        manifest = {}
        mock_async_to_sync.side_effect = lambda f: lambda: manifest
        mock_commit.return_value = True

        commit_manifest_to_chain()

        mock_commit.assert_called_once_with(
            manifest, mock_pylon_client.return_value.__enter__.return_value
        )
