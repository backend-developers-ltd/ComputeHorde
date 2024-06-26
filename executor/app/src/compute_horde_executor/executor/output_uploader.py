from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
import pathlib
import tempfile
import zipfile
from functools import wraps
from typing import Self

import httpx
from compute_horde.em_protocol.miner_requests import OutputUpload, OutputUploadType
from django.conf import settings

logger = logging.getLogger(__name__)

OUTPUT_UPLOAD_TIMEOUT_SECONDS = 300
MAX_NUMBER_OF_FILES = 1000
MAX_CONCURRENT_UPLOADS = 3


class ConcurrencyLimiter:
    def __init__(self, concurrency):
        self.semaphore = asyncio.Semaphore(concurrency)

    async def wrap_task(self, task):
        async with self.semaphore:
            return await task


def retry(max_retries=3, initial_delay=1, backoff_factor=2, exceptions=Exception):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            delay = initial_delay
            for i in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    if i == max_retries - 1:
                        logger.debug(f"Got exception {exc} - but max number of retries reached")
                        raise
                    logger.debug(
                        f"Got exception {exc} but will retry because it is {i + 1} attempt"
                    )
                    await asyncio.sleep(delay)
                    delay *= backoff_factor

        return wrapper

    return decorator


class OutputUploadFailed(Exception):
    def __init__(self, description: str):
        self.description = description


class OutputUploader(metaclass=abc.ABCMeta):
    """Upload the output directory to JobRequest.OutputUpload"""

    def __init__(self, upload_output: OutputUpload):
        self.upload_output = upload_output

    @classmethod
    @abc.abstractmethod
    def handles_output_type(cls) -> OutputUploadType: ...

    @abc.abstractmethod
    async def upload(self, directory: pathlib.Path): ...

    __output_type_map: dict[OutputUploadType, type[OutputUploader]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.__output_type_map[cls.handles_output_type()] = cls

    @classmethod
    def for_upload_output(cls, upload_output: OutputUpload) -> Self:
        return cls.__output_type_map[upload_output.output_upload_type](upload_output)


class ZipAndHTTPPostOutputUploader(OutputUploader):
    """Zip the upload the output directory and HTTP POST the zip file to the given URL"""

    @classmethod
    def handles_output_type(cls) -> OutputUploadType:
        return OutputUploadType.zip_and_http_post

    async def upload(self, directory: pathlib.Path):
        with zipped_directory(directory) as (file_size, fp):
            await upload_post(
                fp,
                "output.zip",
                file_size,
                self.upload_output.url,
                content_type="application/zip",
                form_fields=self.upload_output.form_fields,
            )


class ZipAndHTTPPutOutputUploader(OutputUploader):
    """Zip the upload the output directory and HTTP PUT the zip file to the given URL"""

    @classmethod
    def handles_output_type(cls) -> OutputUploadType:
        return OutputUploadType.zip_and_http_put

    async def upload(self, directory: pathlib.Path):
        with zipped_directory(directory) as (file_size, fp):
            await upload_put(fp, file_size, self.upload_output.url)


class MultiUploadOutputUploader(OutputUploader):
    """Upload multiple files to the specified URLs"""

    @classmethod
    def handles_output_type(cls) -> OutputUploadType:
        return OutputUploadType.multi_upload

    async def upload(self, directory: pathlib.Path):
        single_file_uploads = []
        limiter = ConcurrencyLimiter(MAX_CONCURRENT_UPLOADS)
        tasks = []
        for upload in self.upload_output.uploads:
            file_path = directory / upload.relative_path
            if not file_path.exists():
                raise OutputUploadFailed(f"File not found: {file_path}")

            if upload.output_upload_type == OutputUploadType.single_file_post:
                # we run those concurrently but for loop changes slots - we need to bind
                async def _task(file_path, upload):
                    with file_path.open("rb") as fp:
                        await upload_post(
                            fp,
                            file_path.name,
                            file_path.stat().st_size,
                            upload.url,
                            form_fields=upload.form_fields,
                            headers=upload.signed_headers,
                        )

                tasks.append(limiter.wrap_task(_task(file_path, upload)))
                single_file_uploads.append(upload.relative_path)
            elif upload.output_upload_type == OutputUploadType.single_file_put:
                # we run those concurrently but for loop changes slots - we need to bind
                async def _task(file_path, upload):
                    with file_path.open("rb") as fp:
                        await upload_put(
                            fp, file_path.stat().st_size, upload.url, headers=upload.signed_headers
                        )

                tasks.append(limiter.wrap_task(_task(file_path, upload)))
                single_file_uploads.append(upload.relative_path)
            else:
                raise OutputUploadFailed(f"Unsupported upload type: {upload.output_upload_type}")

        if self.upload_output.system_output:
            if (
                self.upload_output.system_output.output_upload_type
                == OutputUploadType.zip_and_http_post
            ):
                # we don't need to bind any vars because we don't run it in a loop
                async def _task():
                    with zipped_directory(directory, exclude=single_file_uploads) as (
                        file_size,
                        fp,
                    ):
                        await upload_post(
                            fp,
                            "output.zip",
                            file_size,
                            self.upload_output.system_output.url,
                            content_type="application/zip",
                            form_fields=self.upload_output.system_output.form_fields,
                        )

                tasks.append(limiter.wrap_task(_task()))
            elif (
                self.upload_output.system_output.output_upload_type
                == OutputUploadType.zip_and_http_put
            ):
                # we don't need to bind any vars because we don't run it in a loop
                async def _task():
                    with zipped_directory(directory, exclude=single_file_uploads) as (
                        file_size,
                        fp,
                    ):
                        await upload_put(
                            fp,
                            file_size,
                            self.upload_output.system_output.url,
                        )

                tasks.append(limiter.wrap_task(_task()))
            else:
                raise OutputUploadFailed(
                    f"Unsupported system output upload type: {self.upload_output.system_output.output_upload_type}"
                )
        await asyncio.gather(*tasks)


async def make_iterator_async(it):
    """This is stupid."""
    for x in it:
        yield x


@retry(max_retries=3, exceptions=OutputUploadFailed)
async def upload_post(
    fp,
    file_name,
    file_size,
    url,
    content_type="application/octet-stream",
    form_fields=None,
    headers=None,
):
    fp.seek(0)
    async with httpx.AsyncClient() as client:
        form_fields = {
            "Content-Type": content_type,
            **(form_fields or {}),
        }
        files = {"file": (file_name, fp, content_type)}
        headers = {
            "Content-Length": str(file_size),
            **(headers or {}),
        }
        try:
            logger.debug("Upload (POST) file to: %s", url)
            response = await client.post(
                url=url,
                data=form_fields,
                files=files,
                headers=headers,
                timeout=OUTPUT_UPLOAD_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except httpx.HTTPError as ex:
            raise OutputUploadFailed(f"Uploading output failed with http error {ex}")


@retry(max_retries=3, exceptions=OutputUploadFailed)
async def upload_put(fp, file_size, url, headers=None):
    fp.seek(0)
    async with httpx.AsyncClient() as client:
        headers = {
            "Content-Length": str(file_size),
            **(headers or {}),
        }
        try:
            logger.debug("Upload (PUT) file to: %s", url)
            response = await client.put(
                url=url,
                content=make_iterator_async(fp),
                headers=headers,
                timeout=OUTPUT_UPLOAD_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except httpx.HTTPError as ex:
            raise OutputUploadFailed(f"Uploading output failed with http error {ex}")


@contextlib.contextmanager
def zipped_directory(directory: pathlib.Path, exclude=None):
    """
    Context manager that creates a temporary zip file with the files from given directory.
    The temporary file is cleared after the context manager exits.

    Args:
        directory: The directory to zip.
        exclude: A list of relative paths to exclude from the zip file.

    Returns: tuple of size and the file object of the zip file
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

        if file_size > settings.OUTPUT_ZIP_UPLOAD_MAX_SIZE_BYTES:
            raise OutputUploadFailed("Attempting to upload too large file")

        yield file_size, fp
