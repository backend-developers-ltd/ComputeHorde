import pytest
from moto import mock_aws

from compute_horde_validator.validator.s3 import (
    generate_download_url,
    generate_upload_url,
    get_public_url,
    get_s3_client,
)


@pytest.fixture
def bucket_name():
    return "fake_bucket_name"


@pytest.fixture(autouse=True)
def bucket(bucket_name: str):
    with mock_aws():
        client = get_s3_client()
        client.create_bucket(Bucket=bucket_name)
        yield


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
