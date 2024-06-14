import enum
import re
from collections.abc import Mapping
from urllib.parse import urlparse

import pydantic

SAFE_DOMAIN_REGEX = re.compile(r".*")


class OutputUploadType(enum.Enum):
    zip_and_http_post = "zip_and_http_post"
    zip_and_http_put = "zip_and_http_put"
    multi_upload = "multi_upload"
    single_file = "single_file"


class ZipAndHttpPostUpload(pydantic.BaseModel):
    output_upload_type: OutputUploadType = OutputUploadType.zip_and_http_post
    url: str
    form_fields: Mapping[str, str] | None = None


class ZipAndHttpPutUpload(pydantic.BaseModel):
    output_upload_type: OutputUploadType = OutputUploadType.zip_and_http_put
    url: str
    form_fields: Mapping[str, str] | None = None


class SingleFileUpload(pydantic.BaseModel):
    output_upload_type: OutputUploadType = OutputUploadType.single_file
    url: str
    form_fields: Mapping[str, str] | None = None
    relative_path: str

    def is_safe(self) -> bool:
        domain = urlparse(self.url).netloc
        if SAFE_DOMAIN_REGEX.fullmatch(domain):
            return True
        return False


class MultiUpload(pydantic.BaseModel):
    output_upload_type: OutputUploadType = OutputUploadType.multi_upload
    uploads: list[SingleFileUpload]
    # allow custom uploads for stdout and stderr
    system_output: ZipAndHttpPostUpload | ZipAndHttpPutUpload | None = None


OutputUpload = ZipAndHttpPostUpload | ZipAndHttpPutUpload | MultiUpload
