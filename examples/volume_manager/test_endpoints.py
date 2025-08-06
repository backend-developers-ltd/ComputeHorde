import logging
import os
import requests
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VOLUME_MANAGER_PORT = os.environ.get("VOLUME_MANAGER_PORT", "8001")
VOLUME_MANAGER_URL = f"http://localhost:{VOLUME_MANAGER_PORT}"
TEST_MODEL = "prajjwal1/bert-tiny"
JOB_UUID = str(uuid.uuid4())

def test_prepare_volume_endpoint():
    """Test the prepare_volume API contract with valid request."""
    logger.info("Testing prepare_volume endpoint...")
    
    
    # Generate a unique job UUID for each test run
    
    request_data = {
        "job_uuid": JOB_UUID,
        "volume": {
            "volume_type": "huggingface_volume",
            "repo_id": TEST_MODEL,
            "revision": "main",
            "relative_path": "models",
            "usage_type": "reusable"
        },
        "job_metadata": {
            "message_type": "V0JobRequest",
            "job_uuid": JOB_UUID,
            "executor_class": "always_on__llm__a6000",
            "docker_image": "example/image:latest",
            "raw_script": None,
            "docker_run_options_preset": "nvidia_all",
            "docker_run_cmd": ["python", "main.py"],
            "volume": None,
            "output_upload": {
                "output_upload_type": "single_file_put",
                "relative_path": "results.json",
                "url": "https://s3.example.com/results.json"
            },
            "artifacts_dir": "/artifacts"
        }
    }
    
    response = requests.post(f"{VOLUME_MANAGER_URL}/prepare_volume", json=request_data)
    response.raise_for_status()
    
    data = response.json()
    assert data == {"mounts":[["-v","/tmp/volume_manager/cache/hf-prajjwal1_bert-tiny:/volume/models"]]}, data

    logger.info("✓ Prepare volume endpoint working")


def test_job_finished_endpoint():
    """Test the job_finished endpoint."""
    logger.info("Testing job_finished endpoint...")
    
    # Generate a unique job UUID for each test run
    
    request_data = {
        "job_uuid": JOB_UUID
    }
    
    response = requests.post(f"{VOLUME_MANAGER_URL}/job_finished", json=request_data)
    response.raise_for_status()
    
    logger.info("✓ Job finished endpoint working")


def main():
    """Test the required volume manager endpoints."""
    logger.info("Testing required volume manager endpoints...")
    
    # Test basic API endpoints
    test_prepare_volume_endpoint()
    test_job_finished_endpoint()
    
    logger.info("All required endpoints are working! ✓")


if __name__ == "__main__":
    main() 