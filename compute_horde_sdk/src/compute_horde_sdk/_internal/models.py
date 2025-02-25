import base64
import io
import zipfile
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

import pydantic

from _compute_horde_models import output_upload as compute_horde_output_upload
from _compute_horde_models import volume as compute_horde_volume

VOLUME_MOUNT_PATH_PREFIX = "/volume/"
OUTPUT_MOUNT_PATH_PREFIX = "/output/"


class ComputeHordeJobStatus(StrEnum):
    SENT = "Sent"
    ACCEPTED = "Accepted"
    REJECTED = "Rejected"
    COMPLETED = "Completed"
    FAILED = "Failed"

    def is_in_progress(self) -> bool:
        return self in (self.SENT, self.ACCEPTED)


@dataclass
class ComputeHordeJobResult:
    stdout: str
    artifacts: dict[str, bytes]


class FacilitatorJobResponse(pydantic.BaseModel):
    uuid: str
    executor_class: str
    created_at: str
    # last_update: str
    status: ComputeHordeJobStatus
    docker_image: str
    # raw_script: str
    args: list[str]
    env: dict
    # use_gpu: bool
    # hf_repo_id: str
    # hf_revision: str
    # input_url: str
    # output_download_url: str
    # tag: str
    stdout: str
    volumes: list = []
    uploads: list = []
    # target_validator_hotkey: str
    artifacts: dict = {}


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
    def from_file_contents(cls, filename: str, contents: bytes):
        in_memory_output = io.BytesIO()
        zipf = zipfile.ZipFile(in_memory_output, "w")
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

    By default, it downloads the entire repository and copier its structure.
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
