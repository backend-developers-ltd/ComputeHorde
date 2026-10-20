import pytest

from compute_horde_core.output_upload import SingleFilePostUpload, SingleFilePutUpload
from compute_horde_sdk._internal.models import HTTPOutputVolume

URL = "https://example.com/upload"


@pytest.mark.parametrize(
    "mount_path,relative_path",
    [
        ("/output/result.json", "result.json"),
        ("/output/dir/file.bin", "dir/file.bin"),
        ("/output/.hidden", ".hidden"),
    ],
)
def test_valid_output_volume_path_is_converted(mount_path, relative_path):
    assert HTTPOutputVolume(http_method="PUT", url=URL).to_compute_horde_output_upload(
        mount_path
    ) == SingleFilePutUpload(url=URL, relative_path=relative_path)
    assert HTTPOutputVolume(http_method="POST", url=URL).to_compute_horde_output_upload(
        mount_path
    ) == SingleFilePostUpload(url=URL, relative_path=relative_path)


@pytest.mark.parametrize(
    "mount_path",
    [
        "/output",
        "/output/",
        "/output//proc/self/environ",
        "/output/../secret",
        "/output/dir/../../secret",
        "/output/./result.json",
        "/output/dir/",
        "/output/a//b",
        "/output/a\\b",
        "/volume/result.json",
        "output/result.json",
    ],
)
def test_invalid_output_volume_path_is_rejected(mount_path):
    with pytest.raises(ValueError, match="Output volume path"):
        HTTPOutputVolume(http_method="PUT", url=URL).to_compute_horde_output_upload(mount_path)
