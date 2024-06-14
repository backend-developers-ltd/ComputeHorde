import enum
import re
from urllib.parse import urlparse

import pydantic

SAFE_DOMAIN_REGEX = re.compile(r".*")


class VolumeType(enum.Enum):
    inline = "inline"
    zip_url = "zip_url"
    single_file = "single_file"
    multi_volume = "multi_volume"


class InlineVolume(pydantic.BaseModel):
    volume_type: VolumeType = VolumeType.inline
    contents: str
    relative_path: str | None = None

    def is_safe(self) -> bool:
        return True


class ZipUrlVolume(pydantic.BaseModel):
    volume_type: VolumeType = VolumeType.zip_url
    contents: str  # backwards compatible
    relative_path: str | None = None

    def is_safe(self) -> bool:
        domain = urlparse(self.contents).netloc
        if SAFE_DOMAIN_REGEX.fullmatch(domain):
            return True
        return False


class SingleFileVolume(pydantic.BaseModel):
    volume_type: VolumeType = VolumeType.single_file
    url: str
    relative_path: str

    def is_safe(self) -> bool:
        domain = urlparse(self.url).netloc
        if SAFE_DOMAIN_REGEX.fullmatch(domain):
            return True
        return False


class MultiVolume(pydantic.BaseModel):
    volume_type: VolumeType = VolumeType.multi_volume
    volumes: list[InlineVolume | ZipUrlVolume | SingleFileVolume]

    def is_safe(self) -> bool:
        return all(volume.is_safe() for volume in self.volumes)


Volume = InlineVolume | ZipUrlVolume | SingleFileVolume | MultiVolume
