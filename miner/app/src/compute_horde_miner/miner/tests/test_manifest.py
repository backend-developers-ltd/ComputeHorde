import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from django.test import AsyncClient

from compute_horde_miner.miner.views import get_manifest


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_manifest_success():
    """Test HTTP endpoint for getting manifest successfully."""
    expected_manifest = {"executor_class_1": 2, "executor_class_2": 1}
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_manifest = AsyncMock(return_value=expected_manifest)

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_manifest(request)

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["manifest"] == expected_manifest


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_manifest_empty_response():
    """Test HTTP endpoint for getting manifest when executor manager returns empty dict."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_manifest = AsyncMock(return_value={})

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_manifest(request)

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["manifest"] == {}


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_manifest_exception():
    """Test HTTP endpoint for getting manifest when executor manager raises an exception."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_manifest = AsyncMock(side_effect=Exception("Test error"))

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_manifest(request)

        assert response.status_code == 500
        data = json.loads(response.content)
        assert "error" in data
        assert data["error"] == "Could not get manifest"


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_manifest_url():
    """Test that the manifest URL endpoint works correctly."""
    expected_manifest = {"executor_class_1": 2, "executor_class_2": 1}
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_manifest = AsyncMock(return_value=expected_manifest)

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        client = AsyncClient()
        response = await client.get("/v0.1/manifest")

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["manifest"] == expected_manifest
