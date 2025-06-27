import base64
import io
import sys
import zipfile
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

import pydantic

from compute_horde_core import output_upload as compute_horde_output_upload
from compute_horde_core import volume as compute_horde_volume

if sys.version_info >= (3, 11):  # noqa: UP036
    from enum import StrEnum
    from typing import Self
else:
    from backports.strenum import StrEnum  # noqa: UP035
    from typing_extensions import Self  # noqa: UP035

VOLUME_MOUNT_PATH_PREFIX = "/volume/"
OUTPUT_MOUNT_PATH_PREFIX = "/output/"


class ComputeHordeJobStatus(StrEnum):
    """
    Status of a ComputeHorde job.
    """

    SENT = "Sent"
    RECEIVED = "Received"
    ACCEPTED = "Accepted"
    REJECTED = "Rejected"
    STREAMING_READY = "Streaming Ready"
    EXECUTOR_READY = "Executor Ready"
    VOLUMES_READY = "Volumes Ready"
    EXECUTION_DONE = "Execution Done"
    COMPLETED = "Completed"
    FAILED = "Failed"

    @classmethod
    def end_states(cls) -> set["ComputeHordeJobStatus"]:
        """
        Determines which job statuses mean that the job will not be updated anymore.
        """
        return {cls.COMPLETED, cls.FAILED, cls.REJECTED}

    def is_in_progress(self) -> bool:
        """
        Check if the job is in progress (has not completed or failed yet).
        """
        return self not in ComputeHordeJobStatus.end_states()

    def is_successful(self) -> bool:
        """Check if the job has finished successfully."""
        return self == self.COMPLETED

    def is_streaming_ready(self) -> bool:
        """Check if the job is ready for streaming."""
        return self == self.STREAMING_READY

    def is_failed(self) -> bool:
        """Check if the job has failed."""
        return self in (self.FAILED, self.REJECTED)


@dataclass
class ComputeHordeJobResult:
    """
    Result of a ComputeHorde job.
    """

    stdout: str
    """Job standard output."""

    stderr: str
    """Job standard error output."""

    artifacts: dict[str, bytes]
    """Artifact file contents, keyed by file path, as :class:`bytes`."""

    upload_results: dict[str, compute_horde_output_upload.HttpOutputVolumeResponse] = field(default_factory=dict)
    """Service responses for files uploaded to HTTP output volumes, keyed by file name."""

    def add_upload_result(self, path: str, result: compute_horde_output_upload.HttpOutputVolumeResponse) -> None:
        # Mount point is stripped from the upload path when job is being sent to facilitator. Let's add mount point
        # back to the artifact file path for consistency.
        self.upload_results[OUTPUT_MOUNT_PATH_PREFIX + path] = result


class FacilitatorJobResponse(pydantic.BaseModel):
    uuid: str
    executor_class: str
    created_at: str
    # last_update: str
    status: ComputeHordeJobStatus
    docker_image: str
    args: list[str]
    env: dict[str, str]
    # use_gpu: bool
    # hf_repo_id: str
    # hf_revision: str
    # input_url: str
    # output_download_url: str
    # tag: str
    stdout: str
    stderr: str
    # volumes: list = []
    # uploads: list = []
    # target_validator_hotkey: str
    artifacts: dict[str, str] = {}
    upload_results: dict[str, str] = {}
    streaming_server_cert: str | None = None
    streaming_server_address: str | None = None
    streaming_server_port: int | None = None


class FacilitatorJobsResponse(pydantic.BaseModel):
    count: int
    next: str | None = None
    previous: str | None = None
    results: list[FacilitatorJobResponse]


class AbstractInputVolume(ABC):
    def get_volume_relative_path(self, mount_path: str) -> str:
        if not mount_path.startswith(VOLUME_MOUNT_PATH_PREFIX):
            raise ValueError(f"Input volume paths must start with {VOLUME_MOUNT_PATH_PREFIX!r}")
        return mount_path.removeprefix(VOLUME_MOUNT_PATH_PREFIX)

    @abstractmethod
    def to_compute_horde_volume(self, mount_path: str) -> compute_horde_volume.Volume:
        pass


class InlineInputVolume(pydantic.BaseModel, AbstractInputVolume):
    """
    Volume for inline base64 encoded files.
    """

    contents: str
    """Base64 encoded contents of the file"""

    def to_compute_horde_volume(self, mount_path: str) -> compute_horde_volume.InlineVolume:
        relative_path = self.get_volume_relative_path(mount_path)
        return compute_horde_volume.InlineVolume(
            contents=self.contents,
            relative_path=relative_path,
        )

    @classmethod
    def from_file_contents(cls, filename: str, contents: bytes, compress: bool = False) -> Self:
        in_memory_output = io.BytesIO()
        zipf = zipfile.ZipFile(
            in_memory_output, "w", compression=zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED
        )
        zipf.writestr(filename, contents)
        zipf.close()
        in_memory_output.seek(0)
        zip_contents = in_memory_output.read()
        encoded_zip_contents = base64.b64encode(zip_contents).decode()
        return cls(
            contents=encoded_zip_contents,
        )


class HuggingfaceInputVolume(pydantic.BaseModel, AbstractInputVolume):
    """
    Volume for downloading resources from Huggingface.

    By default, it downloads the entire repository and copies its structure.
    To narrow it down, use the ``allow_patterns`` field.
    If a file is inside a subfolder, it will be placed under the same path in the volume.
    """

    repo_id: str
    """Huggingface repository ID, in the format "namespace/name"."""

    repo_type: str | None = None
    """Set to "dataset" or "space" for a dataset or space, None or "model" for a model."""

    revision: str | None = None
    """Git revision ID: branch name / tag / commit hash."""

    allow_patterns: str | list[str] | None = None
    """If provided, only files matching at least one pattern are downloaded."""

    def to_compute_horde_volume(self, mount_path: str) -> compute_horde_volume.HuggingfaceVolume:
        relative_path = self.get_volume_relative_path(mount_path)
        return compute_horde_volume.HuggingfaceVolume(
            relative_path=relative_path,
            repo_id=self.repo_id,
            repo_type=self.repo_type,
            revision=self.revision,
            allow_patterns=self.allow_patterns,
        )


class HTTPInputVolume(pydantic.BaseModel, AbstractInputVolume):
    """Volume for downloading files from the Internet via HTTP."""

    url: str
    """The URL to download the file from."""

    def to_compute_horde_volume(self, mount_path: str) -> compute_horde_volume.SingleFileVolume:
        relative_path = self.get_volume_relative_path(mount_path)
        return compute_horde_volume.SingleFileVolume(
            relative_path=relative_path,
            url=self.url,
        )


InputVolume = InlineInputVolume | HuggingfaceInputVolume | HTTPInputVolume


class HTTPOutputVolume(pydantic.BaseModel):
    """
    Volume for uploading files to the Internet via HTTP.

    Supports both HTTP PUT and POST uploads.

    - For **PUT** uploads, provide the full presigned URL using the `url` parameter.
    - For **POST** uploads, also include any required form fields using the `form_fields` dictionary.

    Examples:
    PUT upload:
        HTTPOutputVolume(
            http_method="PUT",
            url="https://<your-presigned-url>"
        )

    POST upload:
        HTTPOutputVolume(
            http_method="POST",
            url="https://<your-presigned-post-url>",
            form_fields={
                "key": "example.txt",
                "policy": "<base64-policy>",
                "signature": "<signature>",
                "AWSAccessKeyId": "<access-key-id>"
            }
        )

    Note:
        The SDK does **not** generate presigned URLs for you. Use tools like `boto3`, or your S3 provider’s SDK
        to generate the required `url` and optional `form_fields`.

    """

    http_method: Literal["POST", "PUT"]
    """HTTP method to use, can be POST or PUT."""

    url: str
    """The URL to upload the file to."""

    form_fields: Mapping[str, str] | None = None

    signed_headers: Mapping[str, str] | None = None

    def get_volume_relative_path(self, mount_path: str) -> str:
        if not mount_path.startswith(OUTPUT_MOUNT_PATH_PREFIX):
            raise ValueError(f"Output volume paths must start with {OUTPUT_MOUNT_PATH_PREFIX!r}")
        return mount_path.removeprefix(OUTPUT_MOUNT_PATH_PREFIX)

    def to_compute_horde_output_upload(self, mount_path: str) -> compute_horde_output_upload.OutputUpload:
        relative_path = self.get_volume_relative_path(mount_path)

        if self.http_method == "POST":
            return compute_horde_output_upload.SingleFilePostUpload(
                relative_path=relative_path,
                url=self.url,
                form_fields=self.form_fields,
                signed_headers=self.signed_headers,
            )
        elif self.http_method == "PUT":
            if self.form_fields:
                raise ValueError("Form fields are not supported with PUT uploads.")
            return compute_horde_output_upload.SingleFilePutUpload(
                relative_path=relative_path,
                url=self.url,
                signed_headers=self.signed_headers,
            )
        else:
            raise ValueError(f"Unsupported HTTP method: {self.http_method}")


OutputVolume = HTTPOutputVolume
