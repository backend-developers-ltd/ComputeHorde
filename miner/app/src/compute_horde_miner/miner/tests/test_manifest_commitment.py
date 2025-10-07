import pytest
from unittest.mock import AsyncMock, Mock, patch

from compute_horde_core.executor_class import ExecutorClass

from compute_horde_miner.miner.manifest_commitment import (
    MAX_COMMITMENT_LENGTH,
    commit_manifest_to_subtensor,
    format_manifest_commitment,
    has_manifest_changed,
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

    def test_format_manifest_commitment_sorted(self):
        # Test that order is consistent regardless of input order
        manifest1 = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
        }
        manifest2 = {
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
            ExecutorClass.always_on__gpu_24gb: 3,
        }
        assert format_manifest_commitment(manifest1) == format_manifest_commitment(manifest2)

    def test_format_manifest_commitment_no_trailing_semicolon(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 1}
        result = format_manifest_commitment(manifest)
        assert not result.endswith(";")

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
        }
        result = format_manifest_commitment(manifest)
        expected = "always_on.gpu-24gb=2;always_on.llm.a6000=3;spin_up-4min.gpu-24gb=1"
        assert result == expected

    def test_format_manifest_commitment_too_long(self):
        # Create a manifest that would exceed max length
        # This is unlikely in practice but we should test it
        long_manifest = {
            ExecutorClass.always_on__gpu_24gb: 999999999999999999999999999999999999999999999999,
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

    def test_parse_commitment_string_whitespace(self):
        result = parse_commitment_string("   ")
        assert result == {}

    def test_parse_commitment_string_invalid_format(self):
        # Missing equals sign
        result = parse_commitment_string("always_on.gpu-24gb:3")
        assert result == {}

    def test_parse_commitment_string_invalid_executor_class(self):
        # Invalid executor class name
        result = parse_commitment_string("invalid-class=3")
        assert result == {}

    def test_parse_commitment_string_invalid_count(self):
        # Non-numeric count
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

    def test_parse_commitment_string_single(self):
        commitment = "always_on.llm.a6000=5"
        result = parse_commitment_string(commitment)
        expected = {ExecutorClass.always_on__llm__a6000: 5}
        assert result == expected


class TestHasManifestChanged:
    def test_has_manifest_changed_when_different(self):
        current_manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        chain_commitment = "always_on.gpu-24gb=2"
        assert has_manifest_changed(current_manifest, chain_commitment) is True

    def test_has_manifest_changed_when_same(self):
        current_manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        chain_commitment = "always_on.gpu-24gb=3"
        assert has_manifest_changed(current_manifest, chain_commitment) is False

    def test_has_manifest_changed_when_no_chain_commitment(self):
        current_manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        assert has_manifest_changed(current_manifest, None) is True

    def test_has_manifest_changed_when_empty_chain_commitment(self):
        current_manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        assert has_manifest_changed(current_manifest, "") is True

    def test_has_manifest_changed_multiple_classes_same(self):
        current_manifest = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
        }
        chain_commitment = "always_on.gpu-24gb=3;spin_up-4min.gpu-24gb=2"
        assert has_manifest_changed(current_manifest, chain_commitment) is False

    def test_has_manifest_changed_multiple_classes_different_order(self):
        # Order shouldn't matter
        current_manifest = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
        }
        chain_commitment = "spin_up-4min.gpu-24gb=2;always_on.gpu-24gb=3"
        assert has_manifest_changed(current_manifest, chain_commitment) is False

    def test_has_manifest_changed_different_classes(self):
        current_manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        chain_commitment = "always_on.llm.a6000=3"
        assert has_manifest_changed(current_manifest, chain_commitment) is True

    def test_has_manifest_changed_empty_manifest(self):
        current_manifest = {}
        chain_commitment = "always_on.gpu-24gb=3"
        assert has_manifest_changed(current_manifest, chain_commitment) is True


@pytest.mark.asyncio
class TestCommitManifestToSubtensor:
    async def test_commit_manifest_to_subtensor_success(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        wallet = Mock()
        subtensor = Mock()
        subtensor.commit = Mock(return_value=True)
        netuid = 49

        result = await commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is True
        subtensor.commit.assert_called_once_with(wallet, netuid, "always_on.gpu-24gb=3")

    async def test_commit_manifest_to_subtensor_failure(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        wallet = Mock()
        subtensor = Mock()
        subtensor.commit = Mock(return_value=False)
        netuid = 49

        result = await commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is False

    async def test_commit_manifest_to_subtensor_exception(self):
        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        wallet = Mock()
        subtensor = Mock()
        subtensor.commit = Mock(side_effect=Exception("Network error"))
        netuid = 49

        result = await commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is False

    async def test_commit_manifest_to_subtensor_empty_manifest(self):
        manifest = {}
        wallet = Mock()
        subtensor = Mock()
        netuid = 49

        result = await commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is False
        subtensor.commit.assert_not_called()

    async def test_commit_manifest_to_subtensor_too_long(self):
        # Create a manifest that would be too long
        manifest = {
            ExecutorClass.always_on__gpu_24gb: 999999999999999999999999999999999999999999999999,
        }
        wallet = Mock()
        subtensor = Mock()
        netuid = 49

        result = await commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is False
        subtensor.commit.assert_not_called()

    async def test_commit_manifest_to_subtensor_multiple_classes(self):
        manifest = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
        }
        wallet = Mock()
        subtensor = Mock()
        subtensor.commit = Mock(return_value=True)
        netuid = 49

        result = await commit_manifest_to_subtensor(manifest, wallet, subtensor, netuid)

        assert result is True
        expected_commitment = "always_on.gpu-24gb=3;spin_up-4min.gpu-24gb=2"
        subtensor.commit.assert_called_once_with(wallet, netuid, expected_commitment)


class TestRoundTrip:
    """Test that format and parse are inverse operations"""

    def test_round_trip_single_class(self):
        original = {ExecutorClass.always_on__gpu_24gb: 3}
        commitment = format_manifest_commitment(original)
        parsed = parse_commitment_string(commitment)
        assert original == parsed

    def test_round_trip_multiple_classes(self):
        original = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
            ExecutorClass.always_on__llm__a6000: 1,
        }
        commitment = format_manifest_commitment(original)
        parsed = parse_commitment_string(commitment)
        assert original == parsed

    def test_round_trip_empty(self):
        original = {}
        commitment = format_manifest_commitment(original)
        parsed = parse_commitment_string(commitment)
        assert original == parsed


@pytest.mark.django_db
class TestCommitManifestToChainTask:
    """Test the celery task"""

    @patch("compute_horde_miner.miner.tasks.current")
    @patch("compute_horde_miner.miner.tasks.settings")
    @patch("compute_horde_miner.miner.tasks.bittensor")
    @patch("compute_horde_miner.miner.tasks.config")
    @patch("compute_horde_miner.miner.tasks.async_to_sync")
    def test_commit_manifest_to_chain_task_manifest_changed(
        self, mock_async_to_sync, mock_config, mock_bittensor, mock_settings, mock_current
    ):
        from compute_horde_miner.miner.tasks import commit_manifest_to_chain

        # Setup mocks
        mock_config.MANIFEST_COMMITMENT_ENABLED = True

        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_current.executor_manager.get_manifest = AsyncMock(return_value=manifest)

        mock_wallet = Mock()
        mock_wallet.hotkey.ss58_address = "test_hotkey"
        mock_settings.BITTENSOR_WALLET.return_value = mock_wallet
        mock_settings.BITTENSOR_NETWORK = "test"
        mock_settings.BITTENSOR_NETUID = 49

        mock_subtensor_instance = Mock()
        mock_subtensor_instance.get_commitment.return_value = "always_on.gpu-24gb=2"
        mock_bittensor.subtensor.return_value = mock_subtensor_instance

        # Mock async_to_sync to just call the function
        def async_to_sync_impl(func):
            def wrapper(*args, **kwargs):
                if hasattr(func, "__call__"):
                    return func(*args, **kwargs)
                return func

            return wrapper

        mock_async_to_sync.side_effect = async_to_sync_impl

        # Run task
        commit_manifest_to_chain()

        # Verify get_manifest was called
        assert mock_current.executor_manager.get_manifest.called

    @patch("compute_horde_miner.miner.tasks.current")
    @patch("compute_horde_miner.miner.tasks.config")
    def test_commit_manifest_to_chain_task_disabled(self, mock_config, mock_current):
        from compute_horde_miner.miner.tasks import commit_manifest_to_chain

        mock_config.MANIFEST_COMMITMENT_ENABLED = False

        commit_manifest_to_chain()

        # Should not call executor_manager
        assert not mock_current.executor_manager.get_manifest.called

    @patch("compute_horde_miner.miner.tasks.current")
    @patch("compute_horde_miner.miner.tasks.settings")
    @patch("compute_horde_miner.miner.tasks.config")
    @patch("compute_horde_miner.miner.tasks.async_to_sync")
    def test_commit_manifest_to_chain_task_empty_manifest(
        self, mock_async_to_sync, mock_config, mock_settings, mock_current
    ):
        from compute_horde_miner.miner.tasks import commit_manifest_to_chain

        mock_config.MANIFEST_COMMITMENT_ENABLED = True

        # Empty manifest
        manifest = {}
        mock_current.executor_manager.get_manifest = AsyncMock(return_value=manifest)

        def async_to_sync_impl(func):
            def wrapper(*args, **kwargs):
                if hasattr(func, "__call__"):
                    return func(*args, **kwargs)
                return func

            return wrapper

        mock_async_to_sync.side_effect = async_to_sync_impl

        # Run task - should exit early
        commit_manifest_to_chain()

        # Should not try to get wallet or subtensor
        assert not mock_settings.BITTENSOR_WALLET.called

    @patch("compute_horde_miner.miner.tasks.current")
    @patch("compute_horde_miner.miner.tasks.settings")
    @patch("compute_horde_miner.miner.tasks.bittensor")
    @patch("compute_horde_miner.miner.tasks.config")
    @patch("compute_horde_miner.miner.tasks.async_to_sync")
    @patch("compute_horde_miner.miner.tasks.logger")
    def test_commit_manifest_to_chain_task_unchanged_manifest(
        self,
        mock_logger,
        mock_async_to_sync,
        mock_config,
        mock_bittensor,
        mock_settings,
        mock_current,
    ):
        from compute_horde_miner.miner.tasks import commit_manifest_to_chain

        mock_config.MANIFEST_COMMITMENT_ENABLED = True

        manifest = {ExecutorClass.always_on__gpu_24gb: 3}
        mock_current.executor_manager.get_manifest = AsyncMock(return_value=manifest)

        mock_wallet = Mock()
        mock_wallet.hotkey.ss58_address = "test_hotkey"
        mock_settings.BITTENSOR_WALLET.return_value = mock_wallet
        mock_settings.BITTENSOR_NETWORK = "test"
        mock_settings.BITTENSOR_NETUID = 49

        mock_subtensor_instance = Mock()
        # Same as current manifest
        mock_subtensor_instance.get_commitment.return_value = "always_on.gpu-24gb=3"
        mock_bittensor.subtensor.return_value = mock_subtensor_instance

        def async_to_sync_impl(func):
            def wrapper(*args, **kwargs):
                if hasattr(func, "__call__"):
                    return func(*args, **kwargs)
                return func

            return wrapper

        mock_async_to_sync.side_effect = async_to_sync_impl

        # Run task
        commit_manifest_to_chain()

        # Should log that manifest is unchanged
        assert any("unchanged" in str(call).lower() for call in mock_logger.debug.call_args_list)

    @patch("compute_horde_miner.miner.tasks.current")
    @patch("compute_horde_miner.miner.tasks.settings")
    @patch("compute_horde_miner.miner.tasks.bittensor")
    @patch("compute_horde_miner.miner.tasks.config")
    @patch("compute_horde_miner.miner.tasks.async_to_sync")
    @patch("compute_horde_miner.miner.tasks.logger")
    def test_commit_manifest_to_chain_task_exception_handling(
        self,
        mock_logger,
        mock_async_to_sync,
        mock_config,
        mock_bittensor,
        mock_settings,
        mock_current,
    ):
        from compute_horde_miner.miner.tasks import commit_manifest_to_chain

        mock_config.MANIFEST_COMMITMENT_ENABLED = True

        # Make get_manifest raise an exception
        mock_current.executor_manager.get_manifest = AsyncMock(
            side_effect=Exception("Test error")
        )

        def async_to_sync_impl(func):
            def wrapper(*args, **kwargs):
                if hasattr(func, "__call__"):
                    return func(*args, **kwargs)
                return func

            return wrapper

        mock_async_to_sync.side_effect = async_to_sync_impl

        # Run task - should not raise exception
        commit_manifest_to_chain()

        # Should log error
        assert mock_logger.error.called
