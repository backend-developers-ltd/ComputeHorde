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

import huggingface_hub
import httpx
from asgiref.sync import sync_to_async

from compute_horde_core.volume import (
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
    def __init__(self, description: str):
        self.description = description


class VolumeDownloader(metaclass=abc.ABCMeta):
    """Download the volume to the directory."""

    __volume_type_map: dict[type[Volume], Callable[[Volume], VolumeDownloader]] = {}

    @classmethod
    @abc.abstractmethod
    def handles_volume_type(cls) -> type[Volume]: ...

    @abc.abstractmethod
    async def download(self, directory: pathlib.Path): ...

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.__volume_type_map[cls.handles_volume_type()] = lambda download: cls(download)  # type: ignore

    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.max_size_bytes = 2147483648
        self.max_retries = 3

    @classmethod
    def for_volume(cls, volume: Volume) -> VolumeDownloader:
        return cls.__volume_type_map[volume.__class__](volume)


class HuggingfaceVolumeDownloader(VolumeDownloader):
    """Downloads model files from Hugging Face Hub repositories."""

    def __init__(self, volume: HuggingfaceVolume):
        super().__init__()
        self.volume = volume

    @classmethod
    def handles_volume_type(cls):
        return HuggingfaceVolume

    async def download(self, directory: pathlib.Path):
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

    @classmethod
    def _download(
        cls,
        relative_path: pathlib.Path,
        repo_id: str,
        revision: str | None,
        repo_type: str | None = None,
        allow_patterns: str | list[str] | None = None,
        token: str | None = None,
    ):
        try:
            huggingface_hub.snapshot_download(
                repo_id=repo_id,
                repo_type=repo_type,
                revision=revision,
                token=token,
                local_dir=relative_path,
                allow_patterns=allow_patterns,
            )
        except Exception as exc:
            raise VolumeDownloadFailed(str(exc)) from exc


class InlineVolumeDownloader(VolumeDownloader):
    """Downloads and extracts base64 encoded zip contents to directory."""

    def __init__(self, volume: InlineVolume):
        super().__init__()
        self.volume = volume

    @classmethod
    def handles_volume_type(cls):
        return InlineVolume

    async def download(self, directory: pathlib.Path):
        decoded_contents = base64.b64decode(self.volume.contents)
        bytes_io = io.BytesIO(decoded_contents)
        zip_file = zipfile.ZipFile(bytes_io)
        extraction_path = directory
        if self.volume.relative_path:
            extraction_path /= self.volume.relative_path
        zip_file.extractall(extraction_path.as_posix())


class SingleFileVolumeDownloader(VolumeDownloader):
    """Downloads single files from URLs to the target directory."""

    def __init__(self, volume: SingleFileVolume):
        super().__init__()
        self.volume = volume

    @classmethod
    def handles_volume_type(cls):
        return SingleFileVolume

    async def download(self, directory: pathlib.Path):
        file_path = directory / self.volume.relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("wb") as file:
            async with self.semaphore:
                await download_get(
                    file, self.volume.url, max_size_bytes=self.max_size_bytes, max_retries=self.max_retries
                )


class ZipUrlVolumeDownloader(VolumeDownloader):
    """Downloads and extracts zip files from URLs to the target directory."""

    def __init__(self, volume: ZipUrlVolume):
        super().__init__()
        self.volume = volume

    @classmethod
    def handles_volume_type(cls):
        return ZipUrlVolume

    async def download(self, directory: pathlib.Path):
        with tempfile.NamedTemporaryFile() as download_file:
            async with self.semaphore:
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

    def __init__(self, volume: MultiVolume):
        super().__init__()
        self.volume = volume

    @classmethod
    def handles_volume_type(cls):
        return MultiVolume

    async def download(self, directory: pathlib.Path):
        for sub_volume in self.volume.volumes:
            await VolumeDownloader.for_volume(sub_volume).download(directory)


async def download_get(fp, url, max_size_bytes=2147483648, max_retries=3):
    retries = 0
    bytes_received = 0
    backoff_factor = 1

    while retries < max_retries:
        headers = {}
        if bytes_received > 0:
            headers["Range"] = f"bytes={bytes_received}-"

        async with httpx.AsyncClient() as client:
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
