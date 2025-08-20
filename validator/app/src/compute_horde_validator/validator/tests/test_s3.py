from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from moto import mock_aws

from compute_horde_validator.validator.s3 import (
    download_file_content,
    download_prompts_from_s3_url,
    generate_download_url,
    generate_upload_url,
    get_public_url,
    get_s3_client,
    s3_client_context,
)


@pytest.fixture
def bucket_name():
    return "fake_bucket_name"


@pytest.fixture(autouse=True)
def bucket(bucket_name: str):
    mock = mock_aws()
    mock.start()
    
    try:
        with s3_client_context() as client:
            client.create_bucket(Bucket=bucket_name)
        yield
    finally:
        mock.stop()


def test_generate_upload_url(bucket_name: str):
    object_name = "object_name"
    prefix = "prefix/"

    url = generate_upload_url(object_name, bucket_name=bucket_name, prefix=prefix, expiration=3600)

    assert bucket_name in url
    assert object_name in url
    assert prefix in url


def test_generate_upload_url_custom_endpoint_url(bucket_name, settings):
    b2_url = "https://s3.us-west-004.backblazeb2.com"
    settings.AWS_ENDPOINT_URL = "https://s3.us-west-004.backblazeb2.com"

    url = generate_upload_url("obj", bucket_name=bucket_name, prefix="prefix/", expiration=3600)

    assert url.startswith(b2_url)


def test_generate_download_url(bucket_name: str):
    object_name = "object_name"
    prefix = "prefix/"

    url = generate_download_url(
        object_name, bucket_name=bucket_name, prefix=prefix, expiration=3600
    )

    assert object_name in url
    assert prefix in url


@pytest.mark.parametrize(
    ("key", "prefix", "bucket_name", "endpoint_url", "expected"),
    [
        (
            "obj",
            "prefix/",
            "fake_bucket_name",
            None,
            "https://s3.amazonaws.com/fake_bucket_name/prefix/obj",
        ),
        ("obj", "", "fake_bucket_name", None, "https://s3.amazonaws.com/fake_bucket_name/obj"),
        (
            "obj",
            "prefix/",
            "custom",
            "https://s3.us-west-004.backblazeb2.com",
            "https://s3.us-west-004.backblazeb2.com/custom/prefix/obj",
        ),
    ],
)
def test_get_public_url(
    key: str, prefix: str, bucket_name: str, endpoint_url: str, expected: str, settings
):
    settings.AWS_ENDPOINT_URL = endpoint_url

    assert get_public_url(key, prefix=prefix, bucket_name=bucket_name) == expected


@pytest.mark.parametrize(
    "status_code, content, expected",
    [
        (200, "prompt1\nprompt2\nprompt3", ["prompt1", "prompt2", "prompt3"]),
        (200, "single_prompt", ["single_prompt"]),
        (200, "", []),
        (404, "Not Found", ["Not Found"]),
    ],
)
def test_download_prompts_from_s3_url(status_code, content, expected):
    with patch("requests.get") as mock_get:
        # Mock the requests.get response
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.text = content
        mock_get.return_value = mock_response

        result = download_prompts_from_s3_url("https://fake-s3-url.com/prompts.txt")
        assert result == expected

        mock_get.assert_called_once_with("https://fake-s3-url.com/prompts.txt")
