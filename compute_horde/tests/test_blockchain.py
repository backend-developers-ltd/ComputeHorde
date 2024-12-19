from unittest.mock import patch

from django.core.cache import cache
from django.conf import settings

from compute_horde.blockchain.block_cache import get_current_block
from compute_horde.blockchain.tasks import update_block_cache


def test_block_cache_get_current_block_in_cache():
    cache.set(settings.COMPUTE_HORDE_BLOCK_CACHE_KEY, 123)

    with patch('bittensor.subtensor') as mock_subtensor:
        current_block = get_current_block()

        assert current_block == 123
        mock_subtensor.assert_not_called()


def test_block_cache_get_current_block_not_in_cache():
    with patch('bittensor.subtensor') as mock_subtensor:
        mock_subtensor.block = 123

        current_block = get_current_block()

        assert current_block == 123


def test_tasks_update_block_cache():
    with patch('bittensor.subtensor') as mock_subtensor:
        mock_subtensor.block = 123

        update_block_cache.delay()

        current_block = get_current_block()

        assert current_block == 123

        mock_subtensor.assert_called_once()
