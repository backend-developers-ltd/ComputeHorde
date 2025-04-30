import asyncio
import pathlib
from unittest import mock

import pytest

from compute_horde_core.output_upload import (
    MultiUpload,
    MultiUploadOutputUploader,
    OutputUploader,
    OutputUploadFailed,
    SingleFilePostUpload,
    SingleFilePutUpload,
    ZipAndHTTPPostOutputUploader,
    ZipAndHttpPostUpload,
    ZipAndHTTPPutOutputUploader,
    ZipAndHttpPutUpload,
)


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

        # Content should be a ZIP file
        content = request.read()
        assert b"PK\x03\x04" in content  # ZIP signature
