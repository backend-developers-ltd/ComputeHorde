import string
from unittest.mock import Mock, patch

import pytest
from compute_horde_core.executor_class import ExecutorClass
from pylon_client.v1 import PylonRequestException, PylonResponseException

from compute_horde_miner.miner.manifest_commitment import MAX_COMMITMENT_BYTES, commit_manifest


def pylon_hex(value: str) -> str:
    return "0x" + value.encode("utf-8").hex()


def own_commitment(commitment: str | None, age: int = 0):
    return Mock(
        commitment=commitment, block=Mock(number=20_000), commitment_block_number=20_000 - age
    )


class TestCommitManifest:
    def test_commit_manifest_success(self, mock_pylon_client):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=None
        )

        result = commit_manifest(manifest, mock_pylon_client)

        assert result is True
        mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(b"<M:A3>")

    def test_commit_manifest_failure(self, mock_pylon_client):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=None
        )
        mock_pylon_client.v1.identity.set_commitment.side_effect = PylonResponseException()

        result = commit_manifest(manifest, mock_pylon_client)

        assert result is False

    def test_commit_manifest_exception_propagates(self, mock_pylon_client):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=None
        )
        mock_pylon_client.v1.identity.set_commitment.side_effect = PylonRequestException()

        with pytest.raises(PylonRequestException):
            commit_manifest(manifest, mock_pylon_client)

    def test_commit_manifest_empty_manifest(self, mock_pylon_client):
        manifest = {}
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=None
        )

        result = commit_manifest(manifest, mock_pylon_client)

        assert result is True
        mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(b"<M:>")

    def test_commit_manifest_unchanged_manifest(self, mock_pylon_client):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex("<M:A3>")
        )

        result = commit_manifest(manifest, mock_pylon_client)

        assert result is True
        mock_pylon_client.v1.identity.set_commitment.assert_not_called()

    @pytest.mark.parametrize("age, refresh", [(4_999, False), (5_000, True), (5_001, True)])
    def test_refresh_unchanged_manifest(self, mock_pylon_client, age, refresh):
        identity = mock_pylon_client.v1.identity
        identity.get_own_commitment.return_value = own_commitment(pylon_hex("<M:A3>"), age)
        assert commit_manifest({ExecutorClass.always_on__gpu_24gb: 3}, mock_pylon_client)
        if refresh:
            identity.set_commitment.assert_called_once_with(b"<M:A3>")
        else:
            identity.set_commitment.assert_not_called()

    def test_commit_manifest_replaces_other_data_even_when_manifest_unchanged(
        self, mock_pylon_client
    ):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex("https://example.com<M:A3>")
        )

        result = commit_manifest(manifest, mock_pylon_client)

        assert result is True
        mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(b"<M:A3>")

    @pytest.mark.parametrize("layers", [1, 2, 3])
    @pytest.mark.parametrize("other_data", ["", "<D:https://example.com/old-shield>"])
    @pytest.mark.parametrize("count", [0, 1, 2])
    def test_replaces_nested_commitment(self, mock_pylon_client, layers, other_data, count):
        commitment = other_data + "<M:C1>"
        for _ in range(layers):
            commitment = pylon_hex(commitment) + "<M:C1>"
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex(commitment)
        )
        manifest = {ExecutorClass.always_on__cpu__8c__16gb: count} if count else {}
        expected = (f"<M:C{count}>" if count else "<M:>").encode("utf-8")

        assert commit_manifest(manifest, mock_pylon_client) is True
        mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(expected)

        mock_pylon_client.v1.identity.set_commitment.reset_mock()
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment="0x" + expected.hex()
        )
        assert commit_manifest(manifest, mock_pylon_client) is True
        mock_pylon_client.v1.identity.set_commitment.assert_not_called()

    @pytest.mark.parametrize(
        "old_manifest",
        ["", "<M:A2>", "<M:A3>", "<M:invalid>", "<M:A2><M:A3>", pylon_hex("<M:A3>") + "<M:A3>"],
    )
    @pytest.mark.parametrize("shield_first", [True, False])
    @pytest.mark.parametrize("empty_manifest", [True, False])
    def test_preserves_visible_shield(
        self, mock_pylon_client, old_manifest, shield_first, empty_manifest
    ):
        shield = "<D:https://example.com/é>"
        commitment = shield + old_manifest if shield_first else old_manifest + shield
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex(commitment)
        )
        manifest = {} if empty_manifest else {ExecutorClass.always_on__gpu_24gb: 3}
        expected = shield + ("<M:>" if empty_manifest else "<M:A3>")

        assert commit_manifest(manifest, mock_pylon_client) is True
        if commitment == expected:
            mock_pylon_client.v1.identity.set_commitment.assert_not_called()
        else:
            mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(expected.encode())

        mock_pylon_client.v1.identity.set_commitment.reset_mock()
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex(expected)
        )
        assert commit_manifest(manifest, mock_pylon_client) is True
        mock_pylon_client.v1.identity.set_commitment.assert_not_called()

    @pytest.mark.parametrize(
        "size, success", [(MAX_COMMITMENT_BYTES, True), (MAX_COMMITMENT_BYTES + 1, False)]
    )
    def test_shield_counts_towards_byte_limit(self, mock_pylon_client, size, success):
        shield = "<D:" + "é" * 59 + "x" * (size - 128) + ">"
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex(shield + "<M:A2>")
        )

        assert commit_manifest({ExecutorClass.always_on__gpu_24gb: 3}, mock_pylon_client) is success
        if success:
            mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(
                (shield + "<M:A3>").encode()
            )
        else:
            mock_pylon_client.v1.identity.set_commitment.assert_not_called()

    def test_keeps_full_factory_horde_reference(self, mock_pylon_client):
        shield = "<D:" + "s" * 26 + ">"
        factory = "<F:ghcr.io/backend-developers-ltd/factory-horde-factory@" + "q" * 43 + ">"
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex(shield + factory + "<M:A2>")
        )

        assert commit_manifest({ExecutorClass.always_on__gpu_24gb: 3}, mock_pylon_client)
        mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(
            (shield + factory + "<M:A3>").encode()
        )

    @pytest.mark.parametrize("tag", [t for t in string.ascii_uppercase if t != "M"])
    def test_keeps_envelope_of_any_tag(self, mock_pylon_client, tag):
        envelope = f"<{tag}:some-data@" + "A" * 43 + ">"
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex(envelope + "<M:A2>")
        )

        assert commit_manifest({ExecutorClass.always_on__gpu_24gb: 3}, mock_pylon_client)
        mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(
            (envelope + "<M:A3>").encode()
        )

    @pytest.mark.parametrize(
        "other_data",
        ["https://example.com/é", "0xdeadbeef", "0x123", "0xzz", pylon_hex("other data")],
    )
    def test_update_replaces_other_data(self, mock_pylon_client, other_data):
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex(other_data + "<M:A2>")
        )

        assert commit_manifest({ExecutorClass.always_on__gpu_24gb: 3}, mock_pylon_client)
        mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(b"<M:A3>")

    @pytest.mark.parametrize("commitment", ["0xzz", "0x1", "0xff"])
    def test_invalid_response_is_not_overwritten(self, mock_pylon_client, commitment):
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=commitment
        )

        assert commit_manifest({ExecutorClass.always_on__gpu_24gb: 3}, mock_pylon_client) is False
        mock_pylon_client.v1.identity.set_commitment.assert_not_called()

    @pytest.mark.parametrize(
        "size, success", [(MAX_COMMITMENT_BYTES, True), (MAX_COMMITMENT_BYTES + 1, False)]
    )
    def test_commitment_byte_limit(self, mock_pylon_client, size, success):
        count = 10**100
        manifest = f"<M:A{count}>"
        other = "<X:" + "x" * (size - len(manifest) - 4) + ">"
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex(other)
        )

        assert (
            commit_manifest({ExecutorClass.always_on__gpu_24gb: count}, mock_pylon_client)
            is success
        )
        if success:
            mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(
                (other + manifest).encode()
            )
        else:
            mock_pylon_client.v1.identity.set_commitment.assert_not_called()

    OTHER = "<X:other-mechanism@" + "A" * 43 + ">"

    @pytest.mark.parametrize("old_manifest", ["", "<M:A2>", "<M:A3>"])
    @pytest.mark.parametrize("shield", ["", "<D:https://example.com/s>"])
    def test_preserves_other_envelopes(self, mock_pylon_client, old_manifest, shield):
        current = shield + self.OTHER + old_manifest
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex(current)
        )
        expected = shield + self.OTHER + "<M:A3>"

        assert commit_manifest({ExecutorClass.always_on__gpu_24gb: 3}, mock_pylon_client) is True
        if current == expected:
            mock_pylon_client.v1.identity.set_commitment.assert_not_called()
        else:
            mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(
                expected.encode("utf-8")
            )

    def test_moves_other_envelopes_before_manifest(self, mock_pylon_client):
        mock_pylon_client.v1.identity.get_own_commitment.return_value = own_commitment(
            commitment=pylon_hex("<M:A3>" + self.OTHER)
        )

        assert commit_manifest({ExecutorClass.always_on__gpu_24gb: 3}, mock_pylon_client) is True
        mock_pylon_client.v1.identity.set_commitment.assert_called_once_with(
            (self.OTHER + "<M:A3>").encode("utf-8")
        )


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
