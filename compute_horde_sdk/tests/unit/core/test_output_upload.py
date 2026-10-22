import asyncio
import calendar
import io
import os
import pathlib
import zipfile
from unittest import mock

import httpx
import pydantic
import pytest
from pytest_httpx import IteratorStream

from compute_horde_core.output_upload import (
    MultiUpload,
    MultiUploadOutputUploader,
    OutputUpload,
    OutputUploader,
    OutputUploadFailed,
    SingleFilePostUpload,
    SingleFilePutUpload,
    ZipAndHTTPPostOutputUploader,
    ZipAndHttpPostUpload,
    ZipAndHTTPPutOutputUploader,
    ZipAndHttpPutUpload,
)
from compute_horde_core.output_upload._uploader import _zip_date_time, read_stream_with_cap


class TestOutputUploader:
    @pytest.fixture
    def mock_output_upload(self):
        class MockOutputUpload:
            output_upload_type = "mock"

        return MockOutputUpload()

    @pytest.fixture
    def mock_output_uploader(self, mock_output_upload):
        class MockOutputUploader(OutputUploader):
            def __init__(self, upload):
                super().__init__()
                self.upload = upload
                self.upload_called = False

            @classmethod
            def handles_output_type(cls):
                return type(mock_output_upload)

            async def upload(self, directory: pathlib.Path):
                self.upload_called = True

        return MockOutputUploader(mock_output_upload)

    def test_output_uploader_registry(self, mock_output_upload, mock_output_uploader):
        """Test that output uploaders are correctly registered."""
        # Register our mock uploader
        uploader = OutputUploader.for_upload_output(mock_output_upload)

        assert isinstance(uploader, mock_output_uploader.__class__)
        assert uploader.max_size_bytes == 2147483648

    def test_output_upload_failed_exception(self):
        """Test the OutputUploadFailed exception."""
        exception = OutputUploadFailed("Test failure")
        assert str(exception) == "Test failure"


class TestZipAndHTTPPostOutputUploader:
    @pytest.fixture
    def upload(self):
        return ZipAndHttpPostUpload(url="https://example.com/upload", form_fields={"token": "test-token"})

    @pytest.fixture
    def test_file_content(self):
        return b"Test file content"

    @pytest.fixture
    def setup_test_dir(self, tmp_path, test_file_content):
        # Create test files
        test_file = tmp_path / "test_file.txt"
        test_file.write_bytes(test_file_content)

        # Create a subdirectory with a file
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        subdir_file = subdir / "subdir_file.txt"
        subdir_file.write_bytes(test_file_content)

        return tmp_path

    @pytest.mark.asyncio
    async def test_upload(self, upload, setup_test_dir, httpx_mock):
        """Test that the uploader zips and uploads the directory."""
        # Mock response for the POST request
        httpx_mock.add_response(url="https://example.com/upload", method="POST", status_code=200)

        uploader = ZipAndHTTPPostOutputUploader(upload)
        await uploader.upload(setup_test_dir)

        # Verify that a POST request was made
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        request = requests[0]

        # Verify HTTP method
        assert request.method == "POST"

        # Verify request contains form data
        assert b'name="token"' in request.content
        assert b"test-token" in request.content

        # Verify request contains zip file data
        assert b'name="file"' in request.content
        assert b'filename="output.zip"' in request.content

        # ZIP file signature should be in the content
        assert b"PK\x03\x04" in request.content

    @pytest.mark.asyncio
    async def test_upload_empty_dir(self, upload, tmp_path, httpx_mock):
        """Test uploading an empty directory."""
        # Mock response for the POST request
        httpx_mock.add_response(url="https://example.com/upload", method="POST", status_code=200)

        uploader = ZipAndHTTPPostOutputUploader(upload)
        await uploader.upload(tmp_path)

        # Verify that a POST request was made
        requests = httpx_mock.get_requests()
        assert len(requests) == 1

        # Should contain form fields and an empty zip file
        request = requests[0]
        assert b'name="token"' in request.content
        assert b"test-token" in request.content
        assert b'name="file"' in request.content

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("async_sleep_mock")
    async def test_upload_error(self, upload, setup_test_dir, httpx_mock):
        """Test handling of upload errors."""
        # Mock response with an error status
        httpx_mock.add_response(
            url="https://example.com/upload", method="POST", status_code=500, content=b"Server Error", is_reusable=True
        )

        uploader = ZipAndHTTPPostOutputUploader(upload)

        # The retry decorator should be tried, but will still fail
        with pytest.raises(OutputUploadFailed):
            await uploader.upload(setup_test_dir)

        # Verify that requests were made (initial + retries)
        requests = httpx_mock.get_requests()
        assert len(requests) > 0


class TestZipAndHTTPPutOutputUploader:
    @pytest.fixture
    def upload(self):
        return ZipAndHttpPutUpload(url="https://example.com/upload")

    @pytest.fixture
    def test_file_content(self):
        return b"Test file content"

    @pytest.fixture
    def setup_test_dir(self, tmp_path, test_file_content):
        # Create test files
        test_file = tmp_path / "test_file.txt"
        test_file.write_bytes(test_file_content)

        # Create a subdirectory with a file
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        subdir_file = subdir / "subdir_file.txt"
        subdir_file.write_bytes(test_file_content)

        return tmp_path

    @pytest.mark.asyncio
    async def test_upload(self, upload, setup_test_dir, httpx_mock):
        """Test that the uploader zips and uploads the directory."""
        # Mock response for the PUT request
        httpx_mock.add_response(url="https://example.com/upload", method="PUT", status_code=200)

        uploader = ZipAndHTTPPutOutputUploader(upload)
        await uploader.upload(setup_test_dir)

        # Verify that a PUT request was made
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        request = requests[0]

        # Verify HTTP method
        assert request.method == "PUT"

        # Request content should be just the zip file (no form data)
        content = request.read()

        # ZIP file signature should be in the content
        assert b"PK\x03\x04" in content

    @pytest.mark.asyncio
    async def test_upload_empty_dir(self, upload, tmp_path, httpx_mock):
        """Test uploading an empty directory."""
        # Mock response for the PUT request
        httpx_mock.add_response(url="https://example.com/upload", method="PUT", status_code=200)

        uploader = ZipAndHTTPPutOutputUploader(upload)
        await uploader.upload(tmp_path)

        # Verify that a PUT request was made
        requests = httpx_mock.get_requests()
        assert len(requests) == 1

        # Should be a valid zip file (even if empty)
        content = requests[0].read()
        assert b"PK\x05\x06" in content

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("async_sleep_mock")
    async def test_upload_error(self, upload, setup_test_dir, httpx_mock):
        """Test handling of upload errors."""
        # Mock response with an error status
        httpx_mock.add_response(
            url="https://example.com/upload", method="PUT", status_code=500, content=b"Server Error", is_reusable=True
        )

        uploader = ZipAndHTTPPutOutputUploader(upload)

        # The retry decorator should be tried, but will still fail
        with pytest.raises(OutputUploadFailed):
            await uploader.upload(setup_test_dir)

        # Verify that requests were made (initial + retries)
        requests = httpx_mock.get_requests()
        assert len(requests) > 0


class TestMultiUploadOutputUploader:
    @pytest.fixture
    def uploads(self):
        return [
            SingleFilePostUpload(
                url="https://example.com/upload1", relative_path="test_file.txt", form_fields={"token": "test-token-1"}
            ),
            SingleFilePutUpload(url="https://example.com/upload2", relative_path="subdir/subdir_file.txt"),
        ]

    @pytest.fixture
    def system_output(self):
        return ZipAndHttpPostUpload(url="https://example.com/system_output", form_fields={"token": "system-token"})

    @pytest.fixture
    def upload_with_system_output(self, uploads, system_output):
        return MultiUpload(uploads=uploads, system_output=system_output)

    @pytest.fixture
    def upload_without_system_output(self, uploads):
        return MultiUpload(uploads=uploads)

    @pytest.fixture
    def test_file_content(self):
        return b"Test file content"

    @pytest.fixture
    def setup_test_dir(self, tmp_path, test_file_content):
        # Create test files
        test_file = tmp_path / "test_file.txt"
        test_file.write_bytes(test_file_content)

        # Create a subdirectory with a file
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        subdir_file = subdir / "subdir_file.txt"
        subdir_file.write_bytes(test_file_content)

        return tmp_path

    @pytest.mark.asyncio
    async def test_upload_with_system_output(self, upload_with_system_output, setup_test_dir, httpx_mock):
        """Test uploading multiple files with system output."""
        # Mock responses for the HTTP requests
        httpx_mock.add_response(url="https://example.com/upload1", method="POST", status_code=200)
        httpx_mock.add_response(url="https://example.com/upload2", method="PUT", status_code=200)
        httpx_mock.add_response(url="https://example.com/system_output", method="POST", status_code=200)

        uploader = MultiUploadOutputUploader(upload_with_system_output)
        await uploader.upload(setup_test_dir)

        # Verify that all requests were made
        requests = httpx_mock.get_requests()
        assert len(requests) == 3

        # Count request types
        post_requests = [r for r in requests if r.method == "POST"]
        put_requests = [r for r in requests if r.method == "PUT"]
        assert len(post_requests) == 2  # SingleFilePost + SystemOutput
        assert len(put_requests) == 1  # SingleFilePut

        # Verify content of requests
        for request in post_requests:
            if request.url == "https://example.com/upload1":
                # Should contain form data with file
                assert b'name="token"' in request.content
                assert b"test-token-1" in request.content
                assert b'name="file"' in request.content
                assert b'filename="test_file.txt"' in request.content
            elif request.url == "https://example.com/system_output":
                # Should contain form data with ZIP
                assert b'name="token"' in request.content
                assert b"system-token" in request.content
                assert b'name="file"' in request.content
                assert b'filename="output.zip"' in request.content

        # Verify PUT request
        for request in put_requests:
            assert request.url == "https://example.com/upload2"
            # Content should be the file content
            assert request.read() == b"Test file content"

    @pytest.mark.asyncio
    async def test_upload_without_system_output(self, upload_without_system_output, setup_test_dir, httpx_mock):
        """Test uploading multiple files without system output."""
        # Mock responses for the HTTP requests
        httpx_mock.add_response(url="https://example.com/upload1", method="POST", status_code=200)
        httpx_mock.add_response(url="https://example.com/upload2", method="PUT", status_code=200)

        uploader = MultiUploadOutputUploader(upload_without_system_output)
        await uploader.upload(setup_test_dir)

        # Verify that all requests were made
        requests = httpx_mock.get_requests()
        assert len(requests) == 2

        # Count request types
        post_requests = [r for r in requests if r.method == "POST"]
        put_requests = [r for r in requests if r.method == "PUT"]
        assert len(post_requests) == 1  # SingleFilePost
        assert len(put_requests) == 1  # SingleFilePut

    @pytest.mark.asyncio
    async def test_upload_file_not_found(self, upload_without_system_output, setup_test_dir):
        """Test handling of missing file."""
        # Modify the upload to reference a non-existent file
        upload_without_system_output.uploads[0].relative_path = "missing_file.txt"

        uploader = MultiUploadOutputUploader(upload_without_system_output)

        # Should raise an error about the missing file
        with pytest.raises(OutputUploadFailed, match="File not found"):
            await uploader.upload(setup_test_dir)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("async_sleep_mock")
    async def test_upload_error_handling(self, upload_without_system_output, setup_test_dir, httpx_mock):
        """Test error handling during upload."""
        # First request fails, second succeeds
        httpx_mock.add_response(
            url="https://example.com/upload1", method="POST", status_code=500, content=b"Server Error", is_reusable=True
        )
        httpx_mock.add_response(url="https://example.com/upload2", method="PUT", status_code=200)

        uploader = MultiUploadOutputUploader(upload_without_system_output)

        # Should fail due to first request failing
        with pytest.raises(OutputUploadFailed):
            await uploader.upload(setup_test_dir)

    @pytest.fixture
    def upload_with_hanging_and_failing_url(self):
        return MultiUpload(
            uploads=[
                SingleFilePutUpload(url="https://example.com/hanging", relative_path="test_file.txt"),
                SingleFilePutUpload(url="https://example.com/failing", relative_path="subdir/subdir_file.txt"),
            ]
        )

    @pytest.fixture
    def hanging_and_failing_receivers(self, httpx_mock):
        async def never_respond(request):
            await asyncio.Event().wait()

        httpx_mock.add_callback(never_respond, url="https://example.com/hanging")
        httpx_mock.add_response(url="https://example.com/failing", status_code=500, is_reusable=True)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("async_sleep_mock", "hanging_and_failing_receivers")
    async def test_failed_upload_cancels_the_other_uploads(self, upload_with_hanging_and_failing_url, setup_test_dir):
        uploader = MultiUploadOutputUploader(upload_with_hanging_and_failing_url)

        with pytest.raises(OutputUploadFailed):
            await uploader.upload(setup_test_dir)

        assert [task for task in asyncio.all_tasks() if task is not asyncio.current_task()] == []

    @pytest.mark.asyncio
    async def test_concurrency_limit(self, upload_without_system_output, setup_test_dir, httpx_mock):
        """Test that concurrency limit is respected."""
        # Create many uploads to test concurrency
        many_uploads = []
        for i in range(10):
            # Create files
            file_path = setup_test_dir / f"file_{i}.txt"
            file_path.write_text(f"Content {i}")

            # Create upload for each file
            many_uploads.append(
                SingleFilePostUpload(
                    url=f"https://example.com/upload/{i}",
                    relative_path=f"file_{i}.txt",
                    form_fields={"token": f"token-{i}"},
                )
            )

        # Replace uploads in the fixture
        upload_without_system_output.uploads = many_uploads

        # Mock responses for all URLs
        for i in range(10):
            httpx_mock.add_response(url=f"https://example.com/upload/{i}", method="POST", status_code=200)

        # Mock the semaphore to verify it's used correctly
        original_semaphore = asyncio.Semaphore
        semaphore_acquire_count = 0

        class MockSemaphore:
            def __init__(self, value):
                self.sem = original_semaphore(value)
                self.value = value

            async def __aenter__(self):
                nonlocal semaphore_acquire_count
                semaphore_acquire_count += 1
                return await self.sem.__aenter__()

            async def __aexit__(self, *args):
                return await self.sem.__aexit__(*args)

        # Patch the semaphore with our mock
        with mock.patch("compute_horde_core.output_upload.OutputUploader._semaphore", MockSemaphore(3)):
            uploader = MultiUploadOutputUploader(upload_without_system_output)
            await uploader.upload(setup_test_dir)

        # Verify all requests were made
        assert len(httpx_mock.get_requests()) == 10

        # Verify semaphore was used for each upload
        assert semaphore_acquire_count == 10

    @pytest.mark.asyncio
    async def test_system_output_with_zip_http_put(self, uploads, setup_test_dir, httpx_mock):
        """Test uploading with ZipAndHttpPutUpload as system output."""
        system_output = ZipAndHttpPutUpload(url="https://example.com/system_output_put")
        (setup_test_dir / "extra.txt").write_bytes(b"not uploaded separately")

        upload = MultiUpload(uploads=uploads, system_output=system_output)

        # Mock responses for the HTTP requests
        httpx_mock.add_response(url="https://example.com/upload1", method="POST", status_code=200)
        httpx_mock.add_response(url="https://example.com/upload2", method="PUT", status_code=200)
        httpx_mock.add_response(url="https://example.com/system_output_put", method="PUT", status_code=200)

        uploader = MultiUploadOutputUploader(upload)
        await uploader.upload(setup_test_dir)

        # Verify PUT request for system output
        requests = [r for r in httpx_mock.get_requests() if r.url == "https://example.com/system_output_put"]
        assert len(requests) == 1

        request = requests[0]
        assert request.method == "PUT"

        # The zip holds only what the single-file uploads did not cover
        archive = zipfile.ZipFile(io.BytesIO(request.read()))
        assert {name: archive.read(name) for name in archive.namelist()} == {"extra.txt": b"not uploaded separately"}


VALID_RELATIVE_PATHS = [
    "result.json",
    "dir/file.bin",
    "a/b/c.txt",
    "with space.txt",
    ".hidden",
    pytest.param("x" * 255, id="255-byte-name"),
]
INVALID_RELATIVE_PATHS = [
    "",
    "/",
    "/etc/passwd",
    "/proc/self/environ",
    "//x",
    "../secret",
    "a/../secret",
    "./x",
    "a/./b",
    "a//b",
    "dir/",
    "a\\b",
    "a\0b",
    pytest.param("x" * 256, id="256-byte-name"),
    pytest.param("dir/" + "é" * 128, id="256-byte-utf8-name"),
]


class TestSingleFileUploadModels:
    @pytest.mark.parametrize("relative_path", VALID_RELATIVE_PATHS)
    def test_valid_relative_path_is_serialized_unchanged(self, relative_path):
        upload = SingleFilePutUpload(url="https://example.com/upload", relative_path=relative_path)

        assert upload.model_dump() == {
            "output_upload_type": "single_file_put",
            "url": "https://example.com/upload",
            "relative_path": relative_path,
            "signed_headers": None,
        }

    @pytest.mark.parametrize("model", [SingleFilePostUpload, SingleFilePutUpload])
    @pytest.mark.parametrize("relative_path", INVALID_RELATIVE_PATHS)
    def test_invalid_relative_path_is_rejected(self, model, relative_path):
        with pytest.raises(pydantic.ValidationError, match="relative_path"):
            model(url="https://example.com/upload", relative_path=relative_path)

    def test_invalid_relative_path_inside_multi_upload_is_rejected(self):
        with pytest.raises(pydantic.ValidationError, match="relative_path"):
            pydantic.TypeAdapter(OutputUpload).validate_python(
                {
                    "output_upload_type": "multi_upload",
                    "uploads": [
                        {
                            "output_upload_type": "single_file_put",
                            "url": "https://example.com/upload",
                            "relative_path": "/proc/self/environ",
                        }
                    ],
                }
            )


class TestOutputDirectoryConfinement:
    """The uploader must never read anything a job can reach only through the output directory."""

    @pytest.fixture
    def secret_file(self, tmp_path):
        path = tmp_path / "secret"
        path.write_bytes(b"EXECUTOR_TOKEN=do-not-leak")
        return path

    @pytest.fixture
    def output_dir(self, tmp_path):
        path = tmp_path / "output"
        path.mkdir()
        (path / "result.json").write_bytes(b'{"ok": true}')
        return path

    @pytest.fixture
    def hostile_entries(self, output_dir, secret_file):
        os.symlink(secret_file, output_dir / "leak")
        os.symlink("/proc/self/environ", output_dir / "environ")
        os.symlink(secret_file.parent, output_dir / "escape")
        os.mkfifo(output_dir / "fifo")
        (output_dir / "sub").mkdir()
        (output_dir / "sub" / "nested.txt").write_bytes(b"nested")
        (output_dir / "empty-dir").mkdir()

    @pytest.fixture(params=["single_file_put", "single_file_post"])
    def single_file_upload(self, request):
        def make(relative_path):
            return pydantic.TypeAdapter(OutputUpload).validate_python(
                {
                    "output_upload_type": request.param,
                    "url": "https://example.com/upload",
                    "relative_path": relative_path,
                }
            )

        return make

    @pytest.mark.asyncio
    async def test_regular_file_is_uploaded(self, output_dir, single_file_upload, httpx_mock):
        httpx_mock.add_response(url="https://example.com/upload", content=b"stored")
        uploader = OutputUploader.for_upload_output(single_file_upload("result.json"))
        uploader.max_size_bytes = len(b'{"ok": true}')

        result = await uploader.upload(output_dir)

        assert list(result) == ["result.json"]
        assert result["result.json"].body == "stored"
        assert b'{"ok": true}' in httpx_mock.get_requests()[0].read()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "relative_path,reason",
        [
            ("leak", "is a symlink"),
            ("environ", "is a symlink"),
            ("escape/secret", "is a symlink"),
            ("fifo", "is a FIFO"),
            ("sub", "is a directory"),
        ],
    )
    async def test_non_regular_entry_is_rejected_before_any_request(
        self, output_dir, hostile_entries, single_file_upload, httpx_mock, relative_path, reason
    ):
        uploader = OutputUploader.for_upload_output(single_file_upload(relative_path))

        with pytest.raises(OutputUploadFailed, match=reason):
            await uploader.upload(output_dir)

        assert httpx_mock.get_requests() == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("relative_path", ["/proc/self/environ", "../secret", "/output/../secret"])
    async def test_unvalidated_escaping_path_is_rejected_before_any_request(
        self, output_dir, secret_file, httpx_mock, relative_path
    ):
        # model_construct skips validation, as a hop running older code would.
        upload = SingleFilePutUpload.model_construct(url="https://example.com/upload", relative_path=relative_path)
        uploader = OutputUploader.for_upload_output(upload)

        with pytest.raises(OutputUploadFailed, match="must"):
            await uploader.upload(output_dir)

        assert httpx_mock.get_requests() == []

    @pytest.mark.asyncio
    async def test_unvalidated_overlong_name_is_rejected_before_any_request(self, output_dir, httpx_mock):
        upload = SingleFilePutUpload.model_construct(url="https://example.com/upload", relative_path="x" * 256)
        uploader = OutputUploader.for_upload_output(upload)

        with pytest.raises(OutputUploadFailed, match="File name too long"):
            await uploader.upload(output_dir)

        assert httpx_mock.get_requests() == []

    @pytest.mark.asyncio
    async def test_file_over_size_limit_is_rejected_before_any_request(
        self, output_dir, single_file_upload, httpx_mock
    ):
        uploader = OutputUploader.for_upload_output(single_file_upload("result.json"))
        uploader.max_size_bytes = len(b'{"ok": true}') - 1

        with pytest.raises(OutputUploadFailed, match="more than the allowed"):
            await uploader.upload(output_dir)

        assert httpx_mock.get_requests() == []

    @pytest.mark.asyncio
    async def test_missing_file_is_rejected_before_any_request(self, output_dir, single_file_upload, httpx_mock):
        uploader = OutputUploader.for_upload_output(single_file_upload("missing.json"))

        with pytest.raises(OutputUploadFailed, match="File not found"):
            await uploader.upload(output_dir)

        assert httpx_mock.get_requests() == []

    @pytest.mark.asyncio
    async def test_multi_upload_rejects_symlink_before_any_request(self, output_dir, hostile_entries, httpx_mock):
        upload = MultiUpload(
            uploads=[
                SingleFilePutUpload(url="https://example.com/upload1", relative_path="result.json"),
                SingleFilePutUpload(url="https://example.com/upload2", relative_path="leak"),
            ],
        )
        uploader = MultiUploadOutputUploader(upload)

        with pytest.raises(OutputUploadFailed, match="is a symlink"):
            await uploader.upload(output_dir)

        assert httpx_mock.get_requests() == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "upload,method",
        [
            (ZipAndHttpPutUpload(url="https://example.com/upload"), "PUT"),
            (ZipAndHttpPostUpload(url="https://example.com/upload"), "POST"),
        ],
    )
    async def test_zip_contains_only_regular_files(self, output_dir, hostile_entries, httpx_mock, upload, method):
        httpx_mock.add_response(url="https://example.com/upload", method=method)
        uploader = OutputUploader.for_upload_output(upload)

        await uploader.upload(output_dir)

        archive = zipfile.ZipFile(io.BytesIO(zip_payload(httpx_mock.get_requests()[0].read())))
        assert {name: archive.read(name) for name in sorted(archive.namelist())} == {
            "result.json": b'{"ok": true}',
            "sub/nested.txt": b"nested",
        }

    @pytest.mark.asyncio
    async def test_system_output_zip_leaves_out_single_file_uploads_and_symlinks(
        self, output_dir, hostile_entries, httpx_mock
    ):
        httpx_mock.add_response(url="https://example.com/upload1", method="PUT")
        httpx_mock.add_response(url="https://example.com/system_output", method="PUT")
        upload = MultiUpload(
            uploads=[SingleFilePutUpload(url="https://example.com/upload1", relative_path="result.json")],
            system_output=ZipAndHttpPutUpload(url="https://example.com/system_output"),
        )

        await MultiUploadOutputUploader(upload).upload(output_dir)

        (request,) = [r for r in httpx_mock.get_requests() if r.url == "https://example.com/system_output"]
        assert zipfile.ZipFile(io.BytesIO(request.read())).namelist() == ["sub/nested.txt"]

    @pytest.fixture
    def backslash_named_file(self, output_dir):
        (output_dir / "a\\b").write_bytes(b"backslash")

    @pytest.fixture
    def non_utf8_named_file(self, output_dir):
        (output_dir / os.fsdecode(b"\xff.bin")).write_bytes(b"undecodable")

    @pytest.mark.asyncio
    async def test_zip_contains_file_with_backslash_in_its_name(self, output_dir, backslash_named_file, httpx_mock):
        httpx_mock.add_response(url="https://example.com/upload", method="PUT")

        await ZipAndHTTPPutOutputUploader(ZipAndHttpPutUpload(url="https://example.com/upload")).upload(output_dir)

        archive = zipfile.ZipFile(io.BytesIO(httpx_mock.get_requests()[0].read()))
        assert {name: archive.read(name) for name in sorted(archive.namelist())} == {
            "a\\b": b"backslash",
            "result.json": b'{"ok": true}',
        }

    @pytest.mark.asyncio
    async def test_system_output_zip_contains_file_with_backslash_in_its_name(
        self, output_dir, backslash_named_file, httpx_mock
    ):
        httpx_mock.add_response(url="https://example.com/upload1", method="PUT")
        httpx_mock.add_response(url="https://example.com/system_output", method="PUT")
        upload = MultiUpload(
            uploads=[SingleFilePutUpload(url="https://example.com/upload1", relative_path="result.json")],
            system_output=ZipAndHttpPutUpload(url="https://example.com/system_output"),
        )

        result = await MultiUploadOutputUploader(upload).upload(output_dir)

        assert sorted(result) == ["result.json", "system_output"]
        (request,) = [r for r in httpx_mock.get_requests() if r.url == "https://example.com/system_output"]
        assert zipfile.ZipFile(io.BytesIO(request.read())).namelist() == ["a\\b"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "upload",
        [
            ZipAndHttpPutUpload(url="https://example.com/upload"),
            MultiUpload(
                uploads=[SingleFilePutUpload(url="https://example.com/upload1", relative_path="result.json")],
                system_output=ZipAndHttpPutUpload(url="https://example.com/system_output"),
            ),
        ],
    )
    async def test_zip_with_name_that_is_not_utf8_fails_before_any_request(
        self, output_dir, non_utf8_named_file, httpx_mock, upload
    ):
        uploader = OutputUploader.for_upload_output(upload)

        with pytest.raises(OutputUploadFailed, match="cannot be zipped"):
            await uploader.upload(output_dir)

        assert httpx_mock.get_requests() == []

    @pytest.fixture
    def directory_nested_101_levels_deep(self, output_dir):
        deepest = output_dir.joinpath(*["d"] * 101)
        deepest.mkdir(parents=True)
        (deepest / "file").write_bytes(b"deep")

    @pytest.mark.asyncio
    async def test_zip_of_deeply_nested_directories_fails_before_any_request(
        self, output_dir, directory_nested_101_levels_deep, httpx_mock
    ):
        uploader = ZipAndHTTPPutOutputUploader(ZipAndHttpPutUpload(url="https://example.com/upload"))

        with pytest.raises(OutputUploadFailed, match="is nested more than 100 directories deep"):
            await uploader.upload(output_dir)

        assert httpx_mock.get_requests() == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mtime,date_time",
        [
            (0, (1980, 1, 1, 0, 0, 0)),
            # Zip stores seconds in 2-second steps.
            (calendar.timegm((2200, 1, 1, 0, 0, 0)), (2107, 12, 31, 23, 59, 58)),
        ],
    )
    async def test_file_with_mtime_outside_zip_date_range_is_zipped(self, output_dir, httpx_mock, mtime, date_time):
        os.utime(output_dir / "result.json", (mtime, mtime))
        httpx_mock.add_response(url="https://example.com/upload", method="PUT")

        await ZipAndHTTPPutOutputUploader(ZipAndHttpPutUpload(url="https://example.com/upload")).upload(output_dir)

        archive = zipfile.ZipFile(io.BytesIO(httpx_mock.get_requests()[0].read()))
        assert archive.getinfo("result.json").date_time == date_time

    @pytest.mark.parametrize("mtime", [1e300, float("nan")])
    def test_mtime_beyond_platform_time_range_gets_first_zip_date(self, mtime):
        assert _zip_date_time(mtime) == (1980, 1, 1, 0, 0, 0)


def zip_payload(request_body: bytes) -> bytes:
    """The zip file from a PUT body or from the `file` part of a multipart POST body."""
    start = request_body.find(b"PK\x03\x04")
    end = request_body.rfind(b"PK\x05\x06")
    return request_body[start : end + 22]


class TestPutUploadBody:
    @pytest.fixture
    def sent_chunk_sizes(self, monkeypatch):
        """Sizes of the request body chunks in the order httpx passes them to the transport."""
        sizes = []

        async def handle_async_request(transport, request):
            sizes.extend([len(chunk) async for chunk in request.stream])
            return httpx.Response(200)

        monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle_async_request)
        return sizes

    @pytest.fixture
    def output_dir(self, tmp_path):
        (tmp_path / "result.bin").write_bytes(b"x" * 150_000)
        return tmp_path

    @pytest.mark.asyncio
    async def test_file_without_newlines_is_sent_in_bounded_chunks(self, output_dir, sent_chunk_sizes):
        upload = SingleFilePutUpload(url="https://example.com/upload", relative_path="result.bin")

        await OutputUploader.for_upload_output(upload).upload(output_dir)

        assert sent_chunk_sizes == [65_536, 65_536, 18_928]


class TestReadStreamWithCap:
    @pytest.fixture
    def chunks_consumed(self):
        return []

    @pytest.fixture
    def endless_response(self, httpx_mock, chunks_consumed):
        def chunks():
            for i in range(1000):
                chunks_consumed.append(i)
                yield b"x" * 60

        httpx_mock.add_response(url="https://example.com/response", stream=IteratorStream(chunks()))

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("endless_response")
    async def test_reading_stops_at_the_first_chunk_over_the_cap(self, chunks_consumed):
        async with httpx.AsyncClient() as client, client.stream("GET", "https://example.com/response") as response:
            with pytest.raises(OutputUploadFailed, match="exceeds allowed limit"):
                await read_stream_with_cap(response, max_allowed_size=100)

        assert chunks_consumed == [0, 1]

    @pytest.mark.asyncio
    async def test_body_within_the_cap_is_returned(self, httpx_mock):
        httpx_mock.add_response(url="https://example.com/response", stream=IteratorStream([b"ab", b"cd"]))

        async with httpx.AsyncClient() as client, client.stream("GET", "https://example.com/response") as response:
            assert await read_stream_with_cap(response, max_allowed_size=4) == "abcd"
