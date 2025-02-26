import logging

import pytest

from ..base.volume import ZipUrlVolume

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "url, expected",
    [
        ("http://199.199.99.99:3000/file.zip", True),
        ("http://google.com/file.zip", True),
        ("https://drive.google.com/uc?export=download&id=1qqq", True),
        ("https://github.com/qqquser/hordetest/blob/main/image.zip", True),
        ("https://horde-model-job.s3.amazonaws.com/test.zip", True),
        (
            "https://raw.githubusercontent.com/backend-developers-ltd/ComputeHorde/master/file.zip",
            True,
        ),
        (
            "https://spanish-translator.s3.amazonaws.com/file.zip?X-Amz-Algorithm=SHA256&X-Amz-Credential=qqq",
            True,
        ),
        ("https://fake.s3.amazonaws.com.fake.com/test.zip", True),
        ("https://something.r2.cloudflarestorage.com/test.zip", True),
    ],
)
def test_volume_is_safe(url: str, expected: bool):
    volume = ZipUrlVolume(contents=url)
    assert volume.is_safe() == expected
