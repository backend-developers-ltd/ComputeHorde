from unittest.mock import MagicMock, patch

import pytest
from celery import Celery
from django.conf import settings
from django.core.cache import cache

from compute_horde.blockchain.block_cache import get_current_block
from compute_horde.blockchain.tasks import update_block_cache


@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield


@pytest.fixture
def celery():
    app = Celery("test")
    app.config_from_object("django.conf:settings", namespace="CELERY")
    yield app
    app.close()


def test_block_cache_get_current_block_in_cache():
    cache.set(settings.COMPUTE_HORDE_BLOCK_CACHE_KEY, 123)

    mock_subtensor = MagicMock()
    with patch("bittensor.subtensor", return_value=mock_subtensor):
        mock_subtensor.get_current_block.return_value = 123

        current_block = get_current_block()

        assert current_block == 123
        mock_subtensor.get_current_block.assert_not_called()


def test_block_cache_get_current_block_not_in_cache():
    mock_subtensor = MagicMock()
    with patch("bittensor.subtensor", return_value=mock_subtensor):
        mock_subtensor.get_current_block.return_value = 123

        current_block = get_current_block()

        assert current_block == 123


def test_tasks_update_block_cache(celery):
    mock_subtensor = MagicMock()
    with patch("bittensor.subtensor", return_value=mock_subtensor):
        mock_subtensor.get_current_block.return_value = 123

        update_block_cache.delay()

        current_block = get_current_block()

        assert current_block == 123

        mock_subtensor.get_current_block.assert_called_once()
