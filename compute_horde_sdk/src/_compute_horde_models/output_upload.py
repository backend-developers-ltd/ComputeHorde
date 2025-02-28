import enum
import re
from collections.abc import Mapping
from typing import Annotated, Literal
from urllib.parse import urlparse

import pydantic
from pydantic import Field

SAFE_DOMAIN_REGEX = re.compile(r".*")


class OutputUploadType(str, enum.Enum):
    zip_and_http_post = "zip_and_http_post"
    zip_and_http_put = "zip_and_http_put"
    multi_upload = "multi_upload"
    single_file_post = "single_file_post"
    single_file_put = "single_file_put"

    def __str__(self):
        return str.__str__(self)


class ZipAndHttpPostUpload(pydantic.BaseModel):
    output_upload_type: Literal[OutputUploadType.zip_and_http_post] = OutputUploadType.zip_and_http_post
    url: str
    form_fields: Mapping[str, str] | None = None


class ZipAndHttpPutUpload(pydantic.BaseModel):
    output_upload_type: Literal[OutputUploadType.zip_and_http_put] = OutputUploadType.zip_and_http_put
    url: str
    # TODO: PUT implementation does not support it - find out why and clean up
    # form_fields: Mapping[str, str] | None = None


class SingleFilePostUpload(pydantic.BaseModel):
    output_upload_type: Literal[OutputUploadType.single_file_post] = OutputUploadType.single_file_post
    url: str
    form_fields: Mapping[str, str] | None = None
    relative_path: str
    signed_headers: Mapping[str, str] | None = None

    def is_safe(self) -> bool:
        domain = urlparse(self.url).netloc
        if SAFE_DOMAIN_REGEX.fullmatch(domain):
            return True
        return False


class SingleFilePutUpload(pydantic.BaseModel):
    output_upload_type: Literal[OutputUploadType.single_file_put] = OutputUploadType.single_file_put
    url: str
    # TODO: PUT implementation does not support it - find out why and clean up
    # form_fields: Mapping[str, str] | None = None
    relative_path: str
    signed_headers: Mapping[str, str] | None = None

    def is_safe(self) -> bool:
        domain = urlparse(self.url).netloc
        if SAFE_DOMAIN_REGEX.fullmatch(domain):
            return True
        return False


SingleFileUpload = Annotated[
    SingleFilePostUpload | SingleFilePutUpload,
    Field(discriminator="output_upload_type"),
]


class MultiUpload(pydantic.BaseModel):
    output_upload_type: Literal[OutputUploadType.multi_upload] = OutputUploadType.multi_upload
    uploads: list[SingleFileUpload]
    # allow custom uploads for stdout and stderr
    system_output: ZipAndHttpPostUpload | ZipAndHttpPutUpload | None = None


OutputUpload = Annotated[
    SingleFilePostUpload | SingleFilePutUpload | ZipAndHttpPostUpload | ZipAndHttpPutUpload | MultiUpload,
    Field(discriminator="output_upload_type"),
]
