import enum
import re
from typing import Annotated, Any, Literal
from urllib.parse import urlparse

import pydantic
from pydantic import Field

SAFE_DOMAIN_REGEX = re.compile(r".*")


class VolumeType(str, enum.Enum):
    inline = "inline"
    zip_url = "zip_url"
    single_file = "single_file"
    multi_volume = "multi_volume"
    huggingface_volume = "huggingface_volume"

    # default __str__ method returns "VolumeType.inline", we want just "inline"
    def __str__(self) -> str:
        return str.__str__(self)


class HuggingfaceVolume(pydantic.BaseModel):
    volume_type: Literal[VolumeType.huggingface_volume] = VolumeType.huggingface_volume
    repo_id: str
    # Set to "dataset" or "space" for a dataset or space, None or "model" for a model.
    repo_type: str | None = None
    # Git revision id: branch name / tag / commit hash
    revision: str | None = None
    relative_path: str | None = None
    # If provided, only files matching at least one pattern are downloaded.
    allow_patterns: str | list[str] | None = None
    token: str | None = None

    def is_safe(self) -> bool:
        return True


class InlineVolume(pydantic.BaseModel):
    volume_type: Literal[VolumeType.inline] = VolumeType.inline
    contents: str = pydantic.Field(repr=False)
    relative_path: str | None = None

    def is_safe(self) -> bool:
        return True


class ZipUrlVolume(pydantic.BaseModel):
    volume_type: Literal[VolumeType.zip_url] = VolumeType.zip_url
    contents: str  # backwards compatibility - this is the URL
    relative_path: str | None = Field(default=None)

    def is_safe(self) -> bool:
        domain = urlparse(self.contents).netloc
        if SAFE_DOMAIN_REGEX.fullmatch(domain):
            return True
        return False


class HTTPInputCachePolicy(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(json_schema_mode_override="validation")

    cache_ttl_seconds: int | None = None
    cache_key: str | None = Field(default=None, validate_default=True)

    @pydantic.field_validator("cache_ttl_seconds", mode="before")
    @classmethod
    def positive_integer_ttl(cls, value: Any) -> int | None:
        return value if type(value) is int and value > 0 else None

    @pydantic.field_validator("cache_key", mode="before")
    @classmethod
    def validate_cache_key(cls, value: Any, info: pydantic.ValidationInfo) -> str | None:
        if info.data.get("cache_ttl_seconds") is None:
            return None
        if not isinstance(value, str) or not value:
            raise ValueError("cache_key must be a nonempty string when cache_ttl_seconds is positive")
        value.encode("utf-8")
        return value

    @pydantic.model_serializer(mode="wrap")
    def _omit_null_cache_fields(self, handler: pydantic.SerializerFunctionWrapHandler) -> dict[str, Any]:
        volume: dict[str, Any] = handler(self)
        for field in ("cache_ttl_seconds", "cache_key"):
            if volume.get(field) is None:
                volume.pop(field, None)
        return volume


class SingleFileVolume(HTTPInputCachePolicy):
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
            InlineVolume | ZipUrlVolume | SingleFileVolume | HuggingfaceVolume,
            Field(discriminator="volume_type"),
        ]
    ]

    def is_safe(self) -> bool:
        return all(volume.is_safe() for volume in self.volumes)


Volume = Annotated[
    InlineVolume | ZipUrlVolume | SingleFileVolume | MultiVolume | HuggingfaceVolume,
    Field(discriminator="volume_type"),
]
