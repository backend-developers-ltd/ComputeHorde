import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from django.test import AsyncClient

from compute_horde_miner.miner.views import get_main_hotkey


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_main_hotkey_success():
    """Test HTTP endpoint for getting main hotkey successfully."""
    expected_main_hotkey = "hotkey1"
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value=expected_main_hotkey)

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_main_hotkey(request)

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["main_hotkey"] == expected_main_hotkey


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_main_hotkey_none_response():
    """Test HTTP endpoint for getting main hotkey when executor manager returns None."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value=None)

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_main_hotkey(request)

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["main_hotkey"] is None


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_main_hotkey_empty_string_response():
    """Test HTTP endpoint for getting main hotkey when executor manager returns empty string."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value="")

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_main_hotkey(request)

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["main_hotkey"] == ""


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_main_hotkey_exception():
    """Test HTTP endpoint for getting main hotkey when executor manager raises an exception."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(side_effect=Exception("Test error"))

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_main_hotkey(request)

        assert response.status_code == 500
        data = json.loads(response.content)
        assert "error" in data
        assert "Test error" in data["error"]


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_main_hotkey_url():
    """Test that the hotkey URL endpoint works correctly."""
    expected_main_hotkey = "hotkey1"
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value=expected_main_hotkey)

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        client = AsyncClient()
        response = await client.get("/v0.1/hotkey")

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["main_hotkey"] == expected_main_hotkey
