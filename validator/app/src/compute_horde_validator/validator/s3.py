import functools

import boto3
from django.conf import settings


def get_s3_client() -> boto3.client:
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
):
    s3_client = get_s3_client()

    return s3_client.generate_presigned_url(
        method,
        Params={"Bucket": bucket_name, "Key": prefix + key},
        ExpiresIn=expiration,
    )


generate_upload_url = functools.partial(_generate_presigned_url, "put_object")
generate_download_url = functools.partial(_generate_presigned_url, "get_object")


def get_public_url(key: str, *, bucket_name: str, prefix: str = "") -> str:
    endpoint_url = settings.AWS_ENDPOINT_URL or "https://s3.amazonaws.com"

    return f"{endpoint_url}/{bucket_name}/{prefix}{key}"
