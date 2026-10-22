from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
import os
import pathlib
import shutil
import tempfile
import time
import zipfile
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable, Iterator
from functools import wraps
from typing import IO, Any, TypeVar

import httpx

from ..untrusted_files import (
    OpenedFile,
    UntrustedFileRejected,
    open_directory,
    open_regular_file,
    walk_regular_files,
)
from ._models import (
    HttpOutputVolumeResponse,
    MultiUpload,
    OutputUpload,
    SingleFilePostUpload,
    SingleFilePutUpload,
    ZipAndHttpPostUpload,
    ZipAndHttpPutUpload,
)

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

OUTPUT_UPLOAD_TIMEOUT_SECONDS = 300
MAX_NUMBER_OF_FILES = 1000
MAX_CONCURRENT_UPLOADS = 3
_PUT_CHUNK_SIZE = 64 * 1024
_ZIP_FIRST_DATE_TIME = (1980, 1, 1, 0, 0, 0)
_ZIP_LAST_DATE_TIME = (2107, 12, 31, 23, 59, 59)


def retry(
    max_retries: int = 3, initial_delay: float = 1, backoff_factor: float = 2, exceptions: type[Exception] = Exception
) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = initial_delay
            for i in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    if i == max_retries - 1:
                        logger.debug(f"Got exception {exc} - but max number of retries reached")
                        raise
                    logger.debug(f"Got exception {exc} but will retry because it is {i + 1} attempt")
                    await asyncio.sleep(delay)
                    delay *= backoff_factor

        return wrapper

    return decorator


class OutputUploadFailed(Exception):
    def __init__(self, description: str):
        self.description = description


class OutputUploader(metaclass=abc.ABCMeta):
    """Upload the output directory to JobRequest.OutputUpload"""

    __output_type_map: dict[type[OutputUpload], Callable[[OutputUpload], OutputUploader]] = {}
    _semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)

    @classmethod
    @abc.abstractmethod
    def handles_output_type(cls) -> type[OutputUpload]: ...

    @abc.abstractmethod
    async def upload(self, directory: pathlib.Path) -> dict[str, HttpOutputVolumeResponse]: ...

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.__output_type_map[cls.handles_output_type()] = lambda upload: cls(upload)  # type: ignore

    def __init__(self) -> None:
        self.max_size_bytes = 2147483648

    @classmethod
    def for_upload_output(cls, upload_output: OutputUpload) -> OutputUploader:
        return cls.__output_type_map[upload_output.__class__](upload_output)


class ZipAndHTTPPostOutputUploader(OutputUploader):
    """Zip the upload the output directory and HTTP POST the zip file to the given URL"""

    def __init__(self, upload_output: ZipAndHttpPostUpload) -> None:
        super().__init__()
        self.upload_output = upload_output

    @classmethod
    def handles_output_type(cls) -> type[OutputUpload]:
        return ZipAndHttpPostUpload

    async def upload(self, directory: pathlib.Path) -> dict[str, HttpOutputVolumeResponse]:
        with open_directory(directory) as dir_fd:
            with zipped_directory(dir_fd, max_size_bytes=self.max_size_bytes) as (file_size, fp):
                async with self._semaphore:
                    file_name = "output.zip"
                    response = await upload_post(
                        fp,
                        file_name,
                        self.upload_output.url,
                        form_fields=self.upload_output.form_fields,
                    )
                    return {file_name: response}


class ZipAndHTTPPutOutputUploader(OutputUploader):
    """Zip the upload the output directory and HTTP PUT the zip file to the given URL"""

    def __init__(self, upload_output: ZipAndHttpPutUpload) -> None:
        super().__init__()
        self.upload_output = upload_output

    @classmethod
    def handles_output_type(cls) -> type[OutputUpload]:
        return ZipAndHttpPutUpload

    async def upload(self, directory: pathlib.Path) -> dict[str, HttpOutputVolumeResponse]:
        with open_directory(directory) as dir_fd:
            with zipped_directory(dir_fd, max_size_bytes=self.max_size_bytes) as (file_size, fp):
                async with self._semaphore:
                    response = await upload_put(fp, file_size, self.upload_output.url)
                    return {"output.zip": response}


class MultiUploadOutputUploader(OutputUploader):
    """Upload multiple files to the specified URLs"""

    def __init__(self, upload_output: MultiUpload):
        super().__init__()
        self.upload_output = upload_output

    @classmethod
    def handles_output_type(cls) -> type[OutputUpload]:
        return MultiUpload

    async def upload(self, directory: pathlib.Path) -> dict[str, HttpOutputVolumeResponse]:
        # Every file is opened and the system output is zipped before the first request, so a bad
        # path fails the upload before anything is sent. When one request fails the others are
        # cancelled, but uploads that already finished stay delivered.
        with contextlib.ExitStack() as stack:
            dir_fd = stack.enter_context(open_directory(directory))
            files = [
                stack.enter_context(open_output_file(dir_fd, upload.relative_path, self.max_size_bytes))
                for upload in self.upload_output.uploads
            ]
            system_output_upload = self.upload_output.system_output
            if system_output_upload:
                exclude = [upload.relative_path for upload in self.upload_output.uploads]
                zip_size, zip_fp = stack.enter_context(
                    zipped_directory(dir_fd, exclude=exclude, max_size_bytes=self.max_size_bytes)
                )

            tasks = []
            for file, upload in zip(files, self.upload_output.uploads):
                async with self._semaphore:
                    tasks.append(upload_single_file(file, upload))
            if system_output_upload:
                async with self._semaphore:
                    tasks.append(self._upload_system_output(system_output_upload, zip_fp, zip_size))

            responses = await _gather_or_cancel(tasks)
        return dict(responses)

    async def _upload_system_output(
        self, upload: ZipAndHttpPostUpload | ZipAndHttpPutUpload, fp: IO[bytes], file_size: int
    ) -> tuple[str, HttpOutputVolumeResponse]:
        if isinstance(upload, ZipAndHttpPostUpload):
            response = await upload_post(fp, "output.zip", upload.url, form_fields=upload.form_fields)
        else:
            response = await upload_put(fp, file_size, upload.url)
        return "system_output", response


class SingleFilePutOutputUploader(OutputUploader):
    def __init__(self, upload_output: SingleFilePutUpload):
        super().__init__()
        self.upload_output = upload_output

    @classmethod
    def handles_output_type(cls) -> type[OutputUpload]:
        return SingleFilePutUpload

    async def upload(self, directory: pathlib.Path) -> dict[str, HttpOutputVolumeResponse]:
        return await upload_single_file_from_directory(directory, self.upload_output, self.max_size_bytes)


class SingleFilePostOutputUploader(OutputUploader):
    def __init__(self, upload_output: SingleFilePostUpload):
        super().__init__()
        self.upload_output = upload_output

    @classmethod
    def handles_output_type(cls) -> type[OutputUpload]:
        return SingleFilePostUpload

    async def upload(self, directory: pathlib.Path) -> dict[str, HttpOutputVolumeResponse]:
        return await upload_single_file_from_directory(directory, self.upload_output, self.max_size_bytes)


@contextlib.contextmanager
def open_output_file(dir_fd: int, relative_path: str, max_size_bytes: int) -> Iterator[OpenedFile]:
    """
    Open the regular file `relative_path` beneath the output directory `dir_fd` for uploading.

    Raises OutputUploadFailed when the file is missing, is not a regular file reachable
    without following symlinks, cannot be opened, or is larger than `max_size_bytes`.
    """
    with contextlib.ExitStack() as stack:
        try:
            file = stack.enter_context(open_regular_file(dir_fd, relative_path))
        except FileNotFoundError:
            raise OutputUploadFailed(f"File not found: {relative_path}")
        except UntrustedFileRejected as e:
            raise OutputUploadFailed(f"Output file {relative_path!r} cannot be uploaded: {e.reason}")
        except OSError as e:
            raise OutputUploadFailed(f"Output file {relative_path!r} cannot be uploaded: {e.strerror}")
        if file.stat.st_size > max_size_bytes:
            raise OutputUploadFailed(
                f"Output file {relative_path!r} is {file.stat.st_size:,} bytes, "
                f"more than the allowed {max_size_bytes:,} bytes"
            )
        yield file


async def upload_single_file_from_directory(
    directory: pathlib.Path, upload: SingleFilePostUpload | SingleFilePutUpload, max_size_bytes: int
) -> dict[str, HttpOutputVolumeResponse]:
    with open_directory(directory) as dir_fd:
        with open_output_file(dir_fd, upload.relative_path, max_size_bytes) as file:
            relative_path, response = await upload_single_file(file, upload)
    return {relative_path: response}


async def upload_single_file(
    file: OpenedFile, upload: SingleFilePostUpload | SingleFilePutUpload
) -> tuple[str, HttpOutputVolumeResponse]:
    with os.fdopen(file.fd, "rb", closefd=False) as fp:
        if isinstance(upload, SingleFilePostUpload):
            response = await upload_post(
                fp,
                pathlib.PurePosixPath(upload.relative_path).name,
                upload.url,
                form_fields=upload.form_fields,
                headers=upload.signed_headers,
            )
        else:
            response = await upload_put(fp, file.stat.st_size, upload.url, headers=upload.signed_headers)
    return upload.relative_path, response


async def _gather_or_cancel(coros: Iterable[Coroutine[Any, Any, _T]]) -> list[_T]:
    """
    Await `coros` concurrently and return their results in order, like `asyncio.gather`.

    When one fails, or the caller is cancelled, the others are cancelled and then awaited.
    If the caller is cancelled during that wait, this stops waiting and raises
    `CancelledError`, and some of the others may still be unwinding.
    """
    tasks = [asyncio.create_task(coro) for coro in coros]
    try:
        return await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks)


async def _read_in_chunks(fp: IO[bytes]) -> AsyncIterator[bytes]:
    while chunk := fp.read(_PUT_CHUNK_SIZE):
        yield chunk


@retry(max_retries=3, exceptions=OutputUploadFailed)
async def upload_post(
    fp: IO[bytes],
    file_name: str,
    url: str,
    form_fields: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> HttpOutputVolumeResponse:
    """
    Uploads a file via HTTP POST and returns the response.

    Args:
        fp: File pointer to the file to upload.
        file_name: The name assigned to the uploaded file.
        url: The target URL for the upload.
        form_fields: Additional form fields to include in the POST.
        headers: Optional headers for the request,

    Returns:
        HttpOutputVolumeResponse: Contains the HTTP response headers and body.

    """
    fp.seek(0)
    async with httpx.AsyncClient() as client:
        files = {"file": (file_name, fp)}
        form_fields = {**(form_fields or {})}
        headers = {**(headers or {})}

        try:
            logger.debug("Upload (POST) file to: %s", url)
            async with client.stream(
                method="POST",
                url=url,
                data=form_fields,
                files=files,
                headers=headers,
                timeout=OUTPUT_UPLOAD_TIMEOUT_SECONDS,
            ) as response:
                response.raise_for_status()
                body = await read_stream_with_cap(response)
                return HttpOutputVolumeResponse(
                    headers=dict(response.headers),
                    body=body,
                )
        except httpx.HTTPError as ex:
            raise OutputUploadFailed(f"Uploading output failed with http error {ex}")


async def read_stream_with_cap(response: httpx.Response, max_allowed_size: int = 10 * 1024 * 1024) -> str:
    """
    Reads the response in a streaming fashion, enforcing a maximum allowed response size.

    Reading stops at the first chunk that takes the total over the limit, so at most
    `max_allowed_size` bytes are ever kept in memory.

    Args:
        response: The httpx response object.
        max_allowed_size: Maximum allowed size in bytes for the response body, defaults to 10MB.

    Returns:
        The response body as a decoded string.

    """
    content_length = response.headers.get("Content-Length")
    if content_length:
        try:
            if int(content_length) > max_allowed_size:
                raise OutputUploadFailed("Response size exceeds allowed limit based on Content-Length header")
        except ValueError:
            # If conversion fails, proceed to check the chunks.
            pass

    total_bytes = 0
    chunks: list[bytes] = []
    async for chunk in response.aiter_bytes():
        total_bytes += len(chunk)
        if total_bytes > max_allowed_size:
            raise OutputUploadFailed("Response size exceeds allowed limit")
        chunks.append(chunk)

    return b"".join(chunks).decode("utf-8")


@retry(max_retries=3, exceptions=OutputUploadFailed)
async def upload_put(
    fp: IO[bytes], file_size: int, url: str, headers: dict[str, str] | None = None
) -> HttpOutputVolumeResponse:
    """
    Uploads a file via HTTP PUT and returns the response.

    Args:
        fp: File pointer to the file to upload.
        file_size: The size of the file.
        url: The target URL for the upload.
        headers: Optional headers for the request,

    Returns:
        HttpOutputVolumeResponse: Contains the HTTP response headers and body.

    """
    fp.seek(0)
    async with httpx.AsyncClient() as client:
        headers = {
            "Content-Length": str(file_size),
            **(headers or {}),
        }
        try:
            logger.debug("Upload (PUT) file to: %s", url)
            async with client.stream(
                method="PUT",
                url=url,
                content=_read_in_chunks(fp),
                headers=headers,
                timeout=OUTPUT_UPLOAD_TIMEOUT_SECONDS,
            ) as response:
                response.raise_for_status()
                body = await read_stream_with_cap(response)
                return HttpOutputVolumeResponse(
                    headers=dict(response.headers),
                    body=body,
                )
        except httpx.HTTPError as ex:
            raise OutputUploadFailed(f"Uploading output failed with http error {ex}")


@contextlib.contextmanager
def zipped_directory(
    dir_fd: int, exclude: list[str] | None = None, max_size_bytes: int = 2147483648
) -> Iterator[tuple[int, IO[bytes]]]:
    """
    Context manager that creates a temporary zip file with the regular files from the given directory.
    The temporary file is cleared after the context manager exits.

    Symlinks and other non-regular entries are left out, so the archive never contains
    anything from outside the directory. Empty directories are left out as well. Any
    failure to build the archive, such as a file name that is not valid UTF-8, raises
    OutputUploadFailed.

    :param dir_fd: Descriptor of the directory to zip, from `open_directory`.
    :param exclude: A list of relative paths to exclude from the zip file.
    :param max_size_bytes: Maximum allowed size of the zip file in bytes. Defaults to 2147483648.

    :return: tuple of size and the file object of the zip file
    """
    with tempfile.TemporaryFile() as fp:
        try:
            _write_zip(fp, dir_fd, exclude=set(exclude or ()))
        except (UntrustedFileRejected, OSError, UnicodeEncodeError) as e:
            raise OutputUploadFailed(f"Output directory cannot be zipped: {e}") from e

        file_size = fp.tell()
        fp.seek(0)

        if file_size > max_size_bytes:
            raise OutputUploadFailed("Attempting to upload too large file")

        yield file_size, fp


def _write_zip(fp: IO[bytes], dir_fd: int, exclude: set[str]) -> None:
    relative_paths = [path for path in walk_regular_files(dir_fd) if path not in exclude]

    if len(relative_paths) > MAX_NUMBER_OF_FILES:
        raise OutputUploadFailed("Attempting to upload too many files")

    with zipfile.ZipFile(fp, mode="w") as zipf:
        for relative_path in relative_paths:
            with open_regular_file(dir_fd, relative_path) as file:
                _add_to_zip(zipf, file)


def _add_to_zip(zipf: zipfile.ZipFile, file: OpenedFile) -> None:
    # Same metadata as ZipFile.write, taken from the open descriptor instead of the path.
    info = zipfile.ZipInfo(file.path, date_time=_zip_date_time(file.stat.st_mtime))
    info.external_attr = (file.stat.st_mode & 0xFFFF) << 16
    info.file_size = file.stat.st_size
    with os.fdopen(file.fd, "rb", closefd=False) as source, zipf.open(info, "w") as target:
        shutil.copyfileobj(source, target)


def _zip_date_time(mtime: float) -> tuple[int, int, int, int, int, int]:
    # Zip dates span 1980 to 2107; clamped as ZipInfo.from_file does with strict_timestamps=False.
    try:
        date_time = time.localtime(mtime)[:6]
    except (OverflowError, OSError, ValueError):
        return _ZIP_FIRST_DATE_TIME
    return min(max(date_time, _ZIP_FIRST_DATE_TIME), _ZIP_LAST_DATE_TIME)
