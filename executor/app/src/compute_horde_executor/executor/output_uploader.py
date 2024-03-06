from __future__ import annotations

import abc
import pathlib
import tempfile
import zipfile
from typing import Self

import httpx
from compute_horde.em_protocol.miner_requests import OutputUpload, OutputUploadType
from django.conf import settings

OUTPUT_UPLOAD_TIMEOUT_SECONDS = 300
MAX_NUMBER_OF_FILES = 1000


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
        files = list(directory.glob('**/*'))
        if len(files) > MAX_NUMBER_OF_FILES:
            raise OutputUploadFailed('Attempting to upload too many files')

        with tempfile.TemporaryFile() as fp:
            with zipfile.ZipFile(fp, mode="w") as zipf:
                for file in files:
                    zipf.write(filename=file, arcname=file.relative_to(directory))

            file_size = fp.tell()
            fp.seek(0)

            if file_size > settings.OUTPUT_ZIP_UPLOAD_MAX_SIZE_BYTES:
                raise OutputUploadFailed('Attempting to upload too large file')

            async with httpx.AsyncClient() as client:
                form_fields = {
                    "Content-Type": "application/zip",
                    **self.upload_output.form_fields,
                }
                files = {"file": ("output.zip", fp, "application/zip")}
                headers = {
                    "Content-Length": str(file_size),
                    "Content-Type": "application/zip",
                }
                try:
                    response = await client.post(
                        url=self.upload_output.url,
                        data=form_fields,
                        files=files,
                        headers=headers,
                        timeout=OUTPUT_UPLOAD_TIMEOUT_SECONDS,
                    )
                    response.raise_for_status()
                except httpx.HTTPError as ex:
                    raise OutputUploadFailed(f'Uploading output failed with http error {ex}')


class ZipAndHTTPPutOutputUploader(OutputUploader):
    """Zip the upload the output directory and HTTP PUT the zip file to the given URL"""

    @classmethod
    def handles_output_type(cls) -> OutputUploadType:
        return OutputUploadType.zip_and_http_put

    async def upload(self, directory: pathlib.Path):
        files = list(directory.glob('**/*'))
        if len(files) > MAX_NUMBER_OF_FILES:
            raise OutputUploadFailed('Attempting to upload too many files')

        with tempfile.TemporaryFile() as fp:
            with zipfile.ZipFile(fp, mode="w") as zipf:
                for file in files:
                    zipf.write(filename=file, arcname=file.relative_to(directory))

            file_size = fp.tell()
            fp.seek(0)

            if file_size > settings.OUTPUT_ZIP_UPLOAD_MAX_SIZE_BYTES:
                raise OutputUploadFailed('Attempting to upload too large file')

            async with httpx.AsyncClient() as client:
                headers = {
                    "Content-Length": str(file_size),
                    "Content-Type": "application/zip",
                }
                try:
                    response = await client.put(
                        url=self.upload_output.url,
                        content=make_iterator_async(fp),
                        headers=headers,
                        timeout=OUTPUT_UPLOAD_TIMEOUT_SECONDS,
                    )
                    response.raise_for_status()
                except httpx.HTTPError as ex:
                    raise OutputUploadFailed(f'Uploading output failed with http error {ex}')


async def make_iterator_async(it):
    """ This is stupid. """
    for x in it:
        yield x
