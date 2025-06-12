from __future__ import annotations

import abc
import asyncio
import base64
import io
import logging
import pathlib
import random
import tempfile
import zipfile
from collections.abc import Callable
from typing import Any

import httpx
import huggingface_hub
from asgiref.sync import sync_to_async
from huggingface_hub.errors import RepositoryNotFoundError, RevisionNotFoundError

from ._models import (
    HuggingfaceVolume,
    InlineVolume,
    MultiVolume,
    SingleFileVolume,
    Volume,
    ZipUrlVolume,
)

logger = logging.getLogger(__name__)

MAX_CONCURRENT_DOWNLOADS = 3


class VolumeDownloadFailed(Exception):
    def __init__(self, description: str, error_detail: str | None = None) -> None:
        self.description = description
        self.error_detail = error_detail

    def __str__(self) -> str:
        if self.error_detail is not None:
            return f"{self.description}: {self.error_detail}"
        return self.description


class VolumeDownloader(metaclass=abc.ABCMeta):
    """Download the volume to the directory."""

    __volume_type_map: dict[type[Volume], Callable[[Volume], VolumeDownloader]] = {}
    _semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    @classmethod
    @abc.abstractmethod
    def handles_volume_type(cls) -> type[Volume]: ...

    @abc.abstractmethod
    async def download(self, directory: pathlib.Path) -> None: ...

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.__volume_type_map[cls.handles_volume_type()] = lambda download: cls(download)  # type: ignore

    def __init__(self) -> None:
        self.max_size_bytes = 2147483648
        self.max_retries = 3

    @classmethod
    def for_volume(cls, volume: Volume) -> VolumeDownloader:
        return cls.__volume_type_map[volume.__class__](volume)


class HuggingfaceVolumeDownloader(VolumeDownloader):
    """Downloads model files from Hugging Face Hub repositories."""

    def __init__(self, volume: HuggingfaceVolume) -> None:
        super().__init__()
        self.volume = volume

    @classmethod
    def handles_volume_type(cls) -> type[Volume]:
        return HuggingfaceVolume

    async def download(self, directory: pathlib.Path) -> None:
        with tempfile.NamedTemporaryFile():
            extraction_path = directory
            if self.volume.relative_path:
                extraction_path /= self.volume.relative_path
            await sync_to_async(self._download)(
                relative_path=extraction_path,
                repo_id=self.volume.repo_id,
                revision=self.volume.revision,
                repo_type=self.volume.repo_type,
                allow_patterns=self.volume.allow_patterns,
                token=self.volume.token,
            )

    def _download(
        self,
        relative_path: pathlib.Path,
        repo_id: str,
        revision: str | None,
        repo_type: str | None = None,
        allow_patterns: str | list[str] | None = None,
        token: str | None = None,
    ) -> None:
        retries = 0
        last_exc: Exception | None = None
        hf_old_enable_hf_transfer = None
        logger.info("Downloading %r from Hugging Face", repo_id)

        try:
            while retries < self.max_retries:
                try:
                    huggingface_hub.snapshot_download(
                        repo_id=repo_id,
                        repo_type=repo_type,
                        revision=revision,
                        token=token,
                        local_dir=relative_path,
                        allow_patterns=allow_patterns,
                    )
                    return
                except (ValueError, RepositoryNotFoundError, RevisionNotFoundError) as e:
                    logger.error(f"Failed to download model from Hugging Face: {e}")
                    last_exc = e
                    break
                except Exception as e:
                    logger.error(f"Failed to download model from Hugging Face: {e}")
                    last_exc = e
                    retries += 1
                    if huggingface_hub.constants.HF_HUB_ENABLE_HF_TRANSFER:
                        # hf_transfer makes downloads faster, but sometimes fails where vanilla download doesn't.
                        logger.info("Disabling hf_transfer for retry")
                        hf_old_enable_hf_transfer = huggingface_hub.constants.HF_HUB_ENABLE_HF_TRANSFER
                        huggingface_hub.constants.HF_HUB_ENABLE_HF_TRANSFER = False

        finally:
            if hf_old_enable_hf_transfer is not None:  # True or False
                huggingface_hub.constants.HF_HUB_ENABLE_HF_TRANSFER = hf_old_enable_hf_transfer

        raise VolumeDownloadFailed(
            f"Failed to download model from Hugging Face after {retries} retries", error_detail=str(last_exc)
        ) from last_exc


class InlineVolumeDownloader(VolumeDownloader):
    """Downloads and extracts base64 encoded zip contents to directory."""

    def __init__(self, volume: InlineVolume) -> None:
        super().__init__()
        self.volume = volume

    @classmethod
    def handles_volume_type(cls) -> type[Volume]:
        return InlineVolume

    async def download(self, directory: pathlib.Path) -> None:
        decoded_contents = base64.b64decode(self.volume.contents)
        bytes_io = io.BytesIO(decoded_contents)
        zip_file = zipfile.ZipFile(bytes_io)
        extraction_path = directory
        if self.volume.relative_path:
            extraction_path /= self.volume.relative_path
        zip_file.extractall(extraction_path.as_posix())


class SingleFileVolumeDownloader(VolumeDownloader):
    """Downloads single files from URLs to the target directory."""

    def __init__(self, volume: SingleFileVolume) -> None:
        super().__init__()
        self.volume = volume

    @classmethod
    def handles_volume_type(cls) -> type[Volume]:
        return SingleFileVolume

    async def download(self, directory: pathlib.Path) -> None:
        file_path = directory / self.volume.relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("wb") as file:
            async with self._semaphore:
                await download_get(
                    file, self.volume.url, max_size_bytes=self.max_size_bytes, max_retries=self.max_retries
                )


class ZipUrlVolumeDownloader(VolumeDownloader):
    """Downloads and extracts zip files from URLs to the target directory."""

    def __init__(self, volume: ZipUrlVolume) -> None:
        super().__init__()
        self.volume = volume

    @classmethod
    def handles_volume_type(cls) -> type[Volume]:
        return ZipUrlVolume

    async def download(self, directory: pathlib.Path) -> None:
        with tempfile.NamedTemporaryFile() as download_file:
            async with self._semaphore:
                await download_get(
                    download_file,
                    self.volume.contents,
                    max_size_bytes=self.max_size_bytes,
                    max_retries=self.max_retries,
                )
            download_file.seek(0)
            zip_file = zipfile.ZipFile(download_file)
            extraction_path = directory
            if self.volume.relative_path:
                extraction_path /= self.volume.relative_path
            zip_file.extractall(extraction_path.as_posix())


class MultiVolumeDownloader(VolumeDownloader):
    """Downloads multiple volumes to the target directory."""

    def __init__(self, volume: MultiVolume) -> None:
        super().__init__()
        self.volume = volume

    @classmethod
    def handles_volume_type(cls) -> type[Volume]:
        return MultiVolume

    async def download(self, directory: pathlib.Path) -> None:
        for sub_volume in self.volume.volumes:
            downloader = VolumeDownloader.for_volume(sub_volume)
            downloader.max_size_bytes = self.max_size_bytes
            downloader.max_retries = self.max_retries
            await downloader.download(directory)


async def download_get(
    fp: io.BufferedWriter | tempfile._TemporaryFileWrapper[bytes],
    url: str,
    max_size_bytes: int = 2147483648,
    max_retries: int = 3,
) -> None:
    retries = 0
    bytes_received = 0
    backoff_factor = 1

    while retries < max_retries:
        headers = {}
        if bytes_received > 0:
            headers["Range"] = f"bytes={bytes_received}-"

        async with httpx.AsyncClient(follow_redirects=True) as client:
            async with client.stream("GET", url, headers=headers) as response:
                if response.status_code == 416:  # Requested Range Not Satisfiable
                    # Server doesn't support resume, start from the beginning
                    fp.seek(0)
                    bytes_received = 0
                    continue
                elif response.status_code != 206 and bytes_received > 0:  # Partial Content
                    # Server doesn't support resume, start from the beginning
                    fp.seek(0)
                    bytes_received = 0
                    continue

                if bytes_received == 0 and (content_length := response.headers.get("Content-Length")) is not None:
                    # check size early if Content-Length is present
                    if 0 < max_size_bytes < int(content_length):
                        raise VolumeDownloadFailed("Input volume too large")

                try:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes():
                        bytes_received += len(chunk)
                        if 0 < max_size_bytes < bytes_received:
                            raise VolumeDownloadFailed("Input volume too large")
                        fp.write(chunk)
                    return  # Download completed successfully
                except (httpx.HTTPError, OSError) as e:
                    retries += 1
                    if retries >= max_retries:
                        raise e

                    # Exponential backoff with jitter
                    backoff_time = backoff_factor * (2 ** (retries - 1))
                    jitter = random.uniform(0, 0.1)  # Add jitter to avoid synchronization issues
                    backoff_time *= 1 + jitter
                    await asyncio.sleep(backoff_time)

                    backoff_factor *= 2  # Double the backoff factor for the next retry

    raise VolumeDownloadFailed(f"Download failed after {max_retries} retries")
