from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
import pathlib
import tempfile
import zipfile
from collections.abc import AsyncIterable, Callable, Iterable, Iterator
from functools import wraps
from typing import IO, Any

import httpx

from ._models import (
    HttpOutputVolumeResponse,
    MultiUpload,
    OutputUpload,
    OutputUploadType,
    SingleFilePostUpload,
    SingleFilePutUpload,
    ZipAndHttpPostUpload,
    ZipAndHttpPutUpload,
)

logger = logging.getLogger(__name__)

OUTPUT_UPLOAD_TIMEOUT_SECONDS = 300
MAX_NUMBER_OF_FILES = 1000
MAX_CONCURRENT_UPLOADS = 3


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
        with zipped_directory(directory, max_size_bytes=self.max_size_bytes) as (file_size, fp):
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
        with zipped_directory(directory, max_size_bytes=self.max_size_bytes) as (file_size, fp):
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
        single_file_uploads = []
        tasks = []
        for upload in self.upload_output.uploads:
            file_path = directory / upload.relative_path
            if not file_path.exists():
                raise OutputUploadFailed(f"File not found: {file_path}")

            if upload.output_upload_type == OutputUploadType.single_file_post:
                # we run those concurrently but for loop changes slots - we need to bind
                async def _single_post_upload_task(
                    file_path: pathlib.Path, upload: SingleFilePostUpload
                ) -> tuple[str, HttpOutputVolumeResponse]:
                    with file_path.open("rb") as fp:
                        response = await upload_post(
                            fp,
                            file_path.name,
                            upload.url,
                            form_fields=upload.form_fields,
                            headers=upload.signed_headers,
                        )
                        return upload.relative_path, response

                async with self._semaphore:
                    tasks.append(_single_post_upload_task(file_path, upload))
                single_file_uploads.append(upload.relative_path)
            elif upload.output_upload_type == OutputUploadType.single_file_put:
                # we run those concurrently but for loop changes slots - we need to bind
                async def _single_put_upload_task(
                    file_path: pathlib.Path, upload: SingleFilePutUpload
                ) -> tuple[str, HttpOutputVolumeResponse]:
                    with file_path.open("rb") as fp:
                        response = await upload_put(
                            fp, file_path.stat().st_size, upload.url, headers=upload.signed_headers
                        )
                        return upload.relative_path, response

                async with self._semaphore:
                    tasks.append(_single_put_upload_task(file_path, upload))
                single_file_uploads.append(upload.relative_path)
            else:
                raise OutputUploadFailed(f"Unsupported upload type: {upload.output_upload_type}")

        system_output_upload = self.upload_output.system_output
        if system_output_upload:
            if isinstance(system_output_upload, ZipAndHttpPostUpload):
                # we don't need to bind any vars because we don't run it in a loop
                async def _output_post_upload_task(
                    upload: ZipAndHttpPostUpload,
                ) -> tuple[str, HttpOutputVolumeResponse]:
                    with zipped_directory(
                        directory, exclude=single_file_uploads, max_size_bytes=self.max_size_bytes
                    ) as (
                        file_size,
                        fp,
                    ):
                        response = await upload_post(
                            fp,
                            "output.zip",
                            upload.url,
                            form_fields=upload.form_fields,
                        )
                        return "system_output", response

                async with self._semaphore:
                    tasks.append(_output_post_upload_task(system_output_upload))
            elif isinstance(system_output_upload, ZipAndHttpPutUpload):
                # we don't need to bind any vars because we don't run it in a loop
                async def _output_put_upload_task(upload: ZipAndHttpPutUpload) -> tuple[str, HttpOutputVolumeResponse]:
                    with zipped_directory(
                        directory, exclude=single_file_uploads, max_size_bytes=self.max_size_bytes
                    ) as (
                        file_size,
                        fp,
                    ):
                        response = await upload_put(
                            fp,
                            file_size,
                            upload.url,
                        )
                        return "system_output", response

                async with self._semaphore:
                    tasks.append(_output_put_upload_task(system_output_upload))
            else:
                raise OutputUploadFailed(
                    f"Unsupported system output upload type: {system_output_upload.output_upload_type}"
                )
        responses = await asyncio.gather(*tasks)
        return {path: result for path, result in responses}


class SingleFilePutOutputUploader(OutputUploader):
    def __init__(self, upload_output: SingleFilePutUpload):
        super().__init__()
        self.upload_output = upload_output

    @classmethod
    def handles_output_type(cls) -> type[OutputUpload]:
        return SingleFilePutUpload

    async def upload(self, directory: pathlib.Path) -> dict[str, HttpOutputVolumeResponse]:
        file_path = directory / self.upload_output.relative_path
        if not file_path.exists():
            raise OutputUploadFailed(f"File not found: {file_path}")
        with file_path.open("rb") as fp:
            response = await upload_put(
                fp, file_path.stat().st_size, self.upload_output.url, headers=self.upload_output.signed_headers
            )
            return {self.upload_output.relative_path: response}


class SingleFilePostOutputUploader(OutputUploader):
    def __init__(self, upload_output: SingleFilePostUpload):
        super().__init__()
        self.upload_output = upload_output

    @classmethod
    def handles_output_type(cls) -> type[OutputUpload]:
        return SingleFilePostUpload

    async def upload(self, directory: pathlib.Path) -> dict[str, HttpOutputVolumeResponse]:
        file_path = directory / self.upload_output.relative_path
        if not file_path.exists():
            raise OutputUploadFailed(f"File not found: {file_path}")
        with file_path.open("rb") as fp:
            response = await upload_post(
                fp,
                file_path.name,
                self.upload_output.url,
                form_fields=self.upload_output.form_fields,
                headers=self.upload_output.signed_headers,
            )
            return {self.upload_output.relative_path: response}


async def make_iterator_async(it: Iterable[Any]) -> AsyncIterable[Any]:
    """
    Make an iterator async.

    This is stupid.
    """
    for x in it:
        yield x


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
        total_bytes += len(chunks)
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
                content=make_iterator_async(fp),
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
    directory: pathlib.Path, exclude: list[str] | None = None, max_size_bytes: int = 2147483648
) -> Iterator[tuple[int, IO[bytes]]]:
    """
    Context manager that creates a temporary zip file with the files from given directory.
    The temporary file is cleared after the context manager exits.

    :param directory: The directory to zip.
    :param exclude: A list of relative paths to exclude from the zip file.
    :param max_size_bytes: Maximum allowed size of the zip file in bytes. Defaults to 2147483648.

    :return: tuple of size and the file object of the zip file
    """
    files = list(directory.glob("**/*"))
    exclude_set = set(exclude) if exclude else set()

    filtered_files = [file for file in files if str(file.relative_to(directory)) not in exclude_set]

    if len(filtered_files) > MAX_NUMBER_OF_FILES:
        raise OutputUploadFailed("Attempting to upload too many files")

    with tempfile.TemporaryFile() as fp:
        with zipfile.ZipFile(fp, mode="w") as zipf:
            for file in filtered_files:
                zipf.write(filename=file, arcname=file.relative_to(directory))

        file_size = fp.tell()
        fp.seek(0)

        if file_size > max_size_bytes:
            raise OutputUploadFailed("Attempting to upload too large file")

        yield file_size, fp
