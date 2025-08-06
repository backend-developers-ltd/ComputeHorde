from typing import List, Optional, Any
from pydantic import BaseModel


class VolumeSpec(BaseModel):
    """Volume specification from the API."""
    volume_type: str
    repo_id: Optional[str] = None
    repo_type: Optional[str] = None
    revision: Optional[str] = None
    relative_path: Optional[str] = None
    allow_patterns: Optional[List[str]] = None
    token: Optional[str] = None
    usage_type: Optional[str] = None
    volumes: Optional[List['VolumeSpec']] = None
    contents: Optional[str] = None     # For inline volumes
    url: Optional[str] = None     # For URL-based volumes


class OutputUpload(BaseModel):
    """Output upload configuration."""
    output_upload_type: str
    relative_path: str
    url: str


class JobMetadata(BaseModel):
    """Job metadata from the API."""
    message_type: Optional[str] = None
    job_uuid: Optional[str] = None
    executor_class: Optional[str] = None
    docker_image: Optional[str] = None
    raw_script: Optional[str] = None
    docker_run_options_preset: Optional[str] = None
    docker_run_cmd: Optional[List[str]] = None
    volume: Optional[Any] = None
    output_upload: Optional[OutputUpload] = None
    artifacts_dir: Optional[str] = None


class PrepareVolumeRequest(BaseModel):
    """Request to prepare a volume."""
    job_uuid: str
    volume: VolumeSpec
    job_metadata: JobMetadata


class PrepareVolumeResponse(BaseModel):
    """Response from prepare volume endpoint."""
    mounts: List[List[str]]  # List of Docker mount options


class JobFinishedRequest(BaseModel):
    """Request to notify job finished."""
    job_uuid: str


class JobFinishedResponse(BaseModel):
    """Response from job finished endpoint."""
    success: bool 