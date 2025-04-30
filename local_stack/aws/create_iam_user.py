#!/usr/bin/env python3
import boto3
import json


def create_iam_user_with_bucket_access(iam_user_name: str, bucket_name: str):
    """
    Creates an IAM user with inline policy granting access to a single S3 bucket, and returns the newly created access
    key ID and secret access key. :param iam_user_name: The name of the IAM user to create :param bucket_name: The name
     of the S3 bucket to grant access to :return: A tuple of (access_key_id, secret_access_key) """
    iam_client = boto3.client("iam")
    # 1. Create the user
    iam_client.create_user(UserName=iam_user_name)
    # 2. Attach an inline policy to the user giving access (list, get, put, delete)
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                 "Action": [
                     "s3:ListBucket",
                     "s3:GetBucketLocation"
                 ],
                 "Resource": f"arn:aws:s3:::{bucket_name}"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload"
                ],
                "Resource": f"arn:aws:s3:::{bucket_name}/*"
            }
        ]
    }
    # Put inline policy
    iam_client.put_user_policy(UserName=iam_user_name, PolicyName="BucketAccessPolicy",
                               PolicyDocument=json.dumps(policy_document))
    # 3. Create an access key
    access_key_response = iam_client.create_access_key(UserName=iam_user_name)
    access_key_id = access_key_response["AccessKey"]["AccessKeyId"]
    secret_access_key = access_key_response["AccessKey"]["SecretAccessKey"]
    return access_key_id, secret_access_key


if __name__ == "__main__":
    # Replace these placeholders with your desired user and bucket:
    USER_NAME = "placeholder"
    BUCKET_NAME = "compute-horde-integration-tests"
    key_id, secret_key = create_iam_user_with_bucket_access(USER_NAME, BUCKET_NAME)
    print("Created IAM user with the following credentials:")
    print(f"Access Key ID: {key_id}")
    print(f"Secret Access Key: {secret_key}")
