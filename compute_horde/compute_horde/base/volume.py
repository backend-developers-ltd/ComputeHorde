import enum
import re
from typing import Annotated, Literal
from urllib.parse import urlparse

import pydantic
from pydantic import Field

SAFE_DOMAIN_REGEX = re.compile(r".*")


class VolumeType(str, enum.Enum):
    inline = "inline"
    zip_url = "zip_url"
    single_file = "single_file"
    multi_volume = "multi_volume"

    def __str__(self):
        return str.__str__(self)


class InlineVolume(pydantic.BaseModel):
    volume_type: Literal[VolumeType.inline] = VolumeType.inline
    contents: str
    relative_path: str | None = None

    def is_safe(self) -> bool:
        return True


class ZipUrlVolume(pydantic.BaseModel):
    volume_type: Literal[VolumeType.zip_url] = VolumeType.zip_url
    contents: str  # backwards compatible
    relative_path: str | None = Field(default=None)

    def is_safe(self) -> bool:
        domain = urlparse(self.contents).netloc
        if SAFE_DOMAIN_REGEX.fullmatch(domain):
            return True
        return False


class SingleFileVolume(pydantic.BaseModel):
    volume_type: Literal[VolumeType.single_file] = VolumeType.single_file
    url: str
    relative_path: str

    def is_safe(self) -> bool:
        domain = urlparse(self.url).netloc
        if SAFE_DOMAIN_REGEX.fullmatch(domain):
            return True
        return False


class MultiVolume(pydantic.BaseModel):
    volume_type: Literal[VolumeType.multi_volume] = VolumeType.multi_volume
    volumes: list[
        Annotated[
            InlineVolume | ZipUrlVolume | SingleFileVolume, Field(discriminator="volume_type")
        ]
    ]

    def is_safe(self) -> bool:
        return all(volume.is_safe() for volume in self.volumes)


Volume = Annotated[
    InlineVolume | ZipUrlVolume | SingleFileVolume | MultiVolume,
    Field(discriminator="volume_type"),
]
