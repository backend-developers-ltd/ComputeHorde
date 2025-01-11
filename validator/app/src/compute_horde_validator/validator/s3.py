import functools
import logging
from typing import Any

import boto3
import httpx
import requests
import tenacity
from botocore.config import Config
from django.conf import settings

logger = logging.getLogger(__name__)


def get_s3_client(
    aws_access_key_id=None,
    aws_secret_access_key=None,
    region_name=None,
    endpoint_url=None,
    signature_version=None,
):
    if aws_access_key_id is None:
        aws_access_key_id = settings.AWS_ACCESS_KEY_ID
    if aws_secret_access_key is None:
        aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY
    if endpoint_url is None:
        endpoint_url = settings.AWS_ENDPOINT_URL

    if signature_version is None:
        signature_version = settings.AWS_SIGNATURE_VERSION

    if signature_version:
        config_kwargs = {'config': Config(signature_version=signature_version)}
    else:
        config_kwargs = {}

    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
        **config_kwargs,
    )


def _generate_presigned_url(
    method: str,
    key: str,
    *,
    bucket_name: str,
    prefix: str = "",
    expiration: int = 3600,
    s3_client: Any = None,
) -> str:
    if s3_client is None:
        s3_client = get_s3_client()

    return s3_client.generate_presigned_url(  # type: ignore
        method,
        Params={"Bucket": bucket_name, "Key": prefix + key},
        ExpiresIn=expiration,
    )


generate_upload_url = functools.partial(_generate_presigned_url, "put_object")
generate_download_url = functools.partial(_generate_presigned_url, "get_object")


def get_public_url(key: str, *, bucket_name: str, prefix: str = "", s3_client: Any = None) -> str:
    if s3_client is None:
        s3_client = get_s3_client()
    endpoint_url = s3_client.meta.endpoint_url
    return f"{endpoint_url}/{bucket_name}/{prefix}{key}"


@tenacity.retry(
    retry=(
        tenacity.retry_if_exception_type(requests.exceptions.ConnectionError)
        | tenacity.retry_if_exception_type(requests.exceptions.Timeout)
    ),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_fixed(1),
)
def upload_prompts_to_s3_url(s3_url: str, content: str):
    response = requests.put(s3_url, data=content)
    response.raise_for_status()


@tenacity.retry(
    retry=(
        tenacity.retry_if_exception_type(requests.exceptions.ConnectionError)
        | tenacity.retry_if_exception_type(requests.exceptions.Timeout)
    ),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_fixed(1),
)
def download_prompts_from_s3_url(s3_url: str) -> list[str]:
    response = requests.get(s3_url)
    response.raise_for_status()
    return response.text.splitlines()


async def download_file_content(s3_url: str) -> bytes:
    async with httpx.AsyncClient() as client:
        response = await client.get(s3_url, timeout=5)
        response.raise_for_status()
        return response.content
