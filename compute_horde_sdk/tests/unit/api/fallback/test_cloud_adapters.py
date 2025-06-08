import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import sky.backends

from compute_horde_sdk._internal.fallback.adaptors.cloud.factory import CloudAdapterFactory
from compute_horde_sdk._internal.fallback.adaptors.cloud.runpod import RunPodAdapter, RunPodClient, RunPodError


@pytest.fixture
def mock_response():
    """Create a mock response object."""
    response = MagicMock()
    response.json.return_value = {"data": "test"}
    response.raise_for_status.return_value = None
    return response


@pytest.fixture
def mock_session(mock_response):
    """Create a mock session object."""
    session = MagicMock()
    session.get.return_value = mock_response
    return session


@pytest.fixture
def runpod_client(mock_session):
    """Create a RunPodClient instance with a mock session."""
    with patch("requests.Session", return_value=mock_session):
        client = RunPodClient(api_key="test_key")
        client.session = mock_session
        return client


class TestRunPodClient:
    def test_init_with_api_key(self):
        """Test initialization with provided API key."""
        client = RunPodClient(api_key="test_key")
        assert client.api_key == "test_key"
        assert client.session.headers["Authorization"] == "Bearer test_key"

    def test_init_with_env_var(self):
        """Test initialization with API key from environment variable."""
        with patch.dict(os.environ, {"RUNPOD_API_KEY": "env_key"}):
            client = RunPodClient()
            assert client.api_key == "env_key"

    def test_init_with_config_file(self):
        """Test initialization with API key from config file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / ".runpod" / "config.toml"
            config_path.parent.mkdir(parents=True)
            config_path.write_text('[default]\napi_key = "config_key"')

            with patch("pathlib.Path.home", return_value=Path(tmpdir)):
                client = RunPodClient()
                assert client.api_key == "config_key"

    def test_init_no_api_key(self):
        """Test initialization with no API key available."""
        with patch.dict(os.environ, {}, clear=True), patch("pathlib.Path.home", return_value=Path("/nonexistent")):
            with pytest.raises(RunPodError, match="No RunPod API key found"):
                RunPodClient()

    def test_get_pods(self, runpod_client, mock_response):
        """Test getting pods."""
        mock_response.json.return_value = [{"id": "pod1"}, {"id": "pod2"}]
        pods = runpod_client.get_pods()
        assert len(pods) == 2
        assert pods[0]["id"] == "pod1"
        assert pods[1]["id"] == "pod2"
        runpod_client.session.get.assert_called_once_with("https://rest.runpod.io/v1/pods")

    def test_get_pod(self, runpod_client, mock_response):
        """Test getting a specific pod."""
        mock_response.json.return_value = {"id": "pod1", "name": "test_pod"}
        pod = runpod_client.get_pod("pod1")
        assert pod["id"] == "pod1"
        assert pod["name"] == "test_pod"
        runpod_client.session.get.assert_called_once_with(
            "https://rest.runpod.io/v1/pods/pod1", params={"includeMachine": True}
        )

    def test_get_pod_port_mapping(self, runpod_client, mock_response):
        """Test getting pod port mappings."""
        mock_response.json.return_value = {"portMappings": {"22": 10022, "80": 10080}}
        mappings = runpod_client.get_pod_port_mapping("pod1")
        assert mappings["22"] == 10022
        assert mappings["80"] == 10080


class TestRunPodAdapter:
    @pytest.fixture
    def mock_handle(self):
        """Create a mock CloudVmRayResourceHandle."""
        handle = MagicMock(spec=sky.backends.CloudVmRayResourceHandle)
        handle.head_ip = "1.2.3.4"
        return handle

    def test_init(self):
        """Test adapter initialization."""
        adapter = RunPodAdapter(api_key="test_key")
        assert adapter._client is not None
        assert adapter._pod_id is None

    def test_set_job_resource_handle_success(self, mock_handle):
        """Test setting job resource handle successfully."""
        with patch("compute_horde_sdk._internal.fallback.adaptors.cloud.runpod.RunPodClient") as mock_client:
            mock_instance = mock_client.return_value
            mock_instance.get_pods.return_value = [{"id": "pod1", "publicIp": "1.2.3.4"}]

            adapter = RunPodAdapter(api_key="test_key")
            adapter.set_job_resource_handle(mock_handle)

            assert adapter._pod_id == "pod1"
            mock_instance.get_pods.assert_called_once()

    def test_set_job_resource_handle_no_match(self, mock_handle):
        """Test setting job resource handle with no matching pod."""
        with patch("compute_horde_sdk._internal.fallback.adaptors.cloud.runpod.RunPodClient") as mock_client:
            mock_instance = mock_client.return_value
            mock_instance.get_pods.return_value = [{"id": "pod1", "publicIp": "5.6.7.8"}]

            adapter = RunPodAdapter(api_key="test_key")
            adapter.set_job_resource_handle(mock_handle)

            assert adapter._pod_id is None
            mock_instance.get_pods.assert_called_once()

    def test_get_ssh_port_success(self):
        """Test getting SSH port successfully."""
        with patch("compute_horde_sdk._internal.fallback.adaptors.cloud.runpod.RunPodClient") as mock_client:
            mock_instance = mock_client.return_value
            mock_instance.get_pod_port_mapping.return_value = {"22": 10022}

            adapter = RunPodAdapter(api_key="test_key")
            adapter._pod_id = "pod1"
            port = adapter.get_ssh_port("22")

            assert port == 10022
            mock_instance.get_pod_port_mapping.assert_called_once_with("pod1")

    def test_get_ssh_port_no_client(self):
        """Test getting SSH port with no client."""
        adapter = RunPodAdapter(api_key="test_key")
        adapter._client = None
        port = adapter.get_ssh_port("22")
        assert port is None

    def test_get_ssh_port_no_pod_id(self):
        """Test getting SSH port with no pod ID."""
        adapter = RunPodAdapter(api_key="test_key")
        port = adapter.get_ssh_port("22")
        assert port is None


class TestCloudAdapterFactory:
    def test_create_adapter_runpod(self):
        """Test creating RunPod adapter."""
        adapter = CloudAdapterFactory.create_adapter("runpod", api_key="test_key")
        assert isinstance(adapter, RunPodAdapter)
        assert adapter._client is not None

    def test_create_adapter_unsupported(self):
        """Test creating adapter for unsupported cloud."""
        adapter = CloudAdapterFactory.create_adapter("unsupported")
        assert adapter is None
