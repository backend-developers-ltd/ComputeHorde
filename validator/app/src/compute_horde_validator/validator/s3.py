import functools
import logging

import boto3
import httpx
import requests
import tenacity
from django.conf import settings

logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        endpoint_url=settings.AWS_ENDPOINT_URL,
    )


def _generate_presigned_url(
    method: str,
    key: str,
    *,
    bucket_name: str,
    prefix: str = "",
    expiration: int = 3600,
) -> str:
    s3_client = get_s3_client()

    return s3_client.generate_presigned_url(  # type: ignore
        method,
        Params={"Bucket": bucket_name, "Key": prefix + key},
        ExpiresIn=expiration,
    )


generate_upload_url = functools.partial(_generate_presigned_url, "put_object")
generate_download_url = functools.partial(_generate_presigned_url, "get_object")


def get_public_url(key: str, *, bucket_name: str, prefix: str = "") -> str:
    endpoint_url = settings.AWS_ENDPOINT_URL or "https://s3.amazonaws.com"
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
