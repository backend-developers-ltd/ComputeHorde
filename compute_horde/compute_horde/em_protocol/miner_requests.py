import enum
from typing import Self

from pydantic import model_validator

from ..base.output_upload import OutputUpload, OutputUploadType  # noqa
from ..base.volume import Volume, VolumeType
from ..base_requests import BaseRequest, JobMixin


class RequestType(enum.Enum):
    V0PrepareJobRequest = "V0PrepareJobRequest"
    V0RunJobRequest = "V0RunJobRequest"
    GenericError = "GenericError"


class BaseMinerRequest(BaseRequest):
    message_type: RequestType


class V0InitialJobRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0PrepareJobRequest
    base_docker_image_name: str | None = None
    timeout_seconds: int | None = None
    volume_type: VolumeType | None = None


class V0JobRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0RunJobRequest
    docker_image_name: str | None = None
    raw_script: str | None = None
    docker_run_options_preset: str
    docker_run_cmd: list[str]
    volume: Volume | None = None
    output_upload: OutputUpload | None = None

    @model_validator(mode="after")
    def validate_at_least_docker_image_or_raw_script(self) -> Self:
        if not (bool(self.docker_image_name) or bool(self.raw_script)):
            raise ValueError("Expected at least one of `docker_image_name` or `raw_script`")
        return self


class GenericError(BaseMinerRequest):
    message_type: RequestType = RequestType.GenericError
    details: str | None = None
