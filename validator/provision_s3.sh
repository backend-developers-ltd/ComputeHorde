#!/bin/bash
set -eu -o pipefail

if ! command -v aws &> /dev/null; then
    echo "AWS CLI cannot be found. Please, install and configure it according to the instructions for your OS: https://aws.amazon.com/cli/"
    exit 1
fi

if ! command -v jq &>/dev/null; then
    echo "jq cannot be found. Please, install it according to the instructions for your OS: https://jqlang.github.io/jq/download/"
    exit 1
fi

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <prompts_bucket_name> <answers_bucket_name> [--create-user]"
    exit 1
fi

PROMPTS_BUCKET=$1
ANSWERS_BUCKET=$2
CREATE_USER=false

for arg in "$@"; do
    if [ "$arg" = "--create-user" ]; then
        CREATE_USER=true
        break
    fi
done

export AWS_PAGER=""
BUCKET_REGION="${AWS_REGION:-us-east-1}"

create_bucket() {
    local bucket_name="$1"

    set +e
    HEAD_OUTPUT=$(aws s3api head-bucket --bucket "$bucket_name" --no-cli-pager 2>&1)
    HEAD_CODE="$?"
    set -e

    if [[ $HEAD_CODE -eq 0 ]]; then
        BUCKET_REGION="$(echo "$HEAD_OUTPUT" | jq --raw-output .BucketRegion)"
        echo "Bucket $bucket_name already exists and is owned by you. Skipping creation."
        return 0
    elif [[ $HEAD_CODE -eq 254 ]] && [[ $HEAD_OUTPUT == *"Not Found"* ]]; then
        # proceed below to create the bucket
        true # no-op
    elif [[ $HEAD_CODE -eq 254 ]] && [[ $HEAD_OUTPUT == *"Forbidden"* ]]; then
        echo "Bucket $bucket_name is owned by someone else. Please choose a different bucket name."
        exit 1
    else
        echo "Failed to create bucket $bucket_name: $HEAD_OUTPUT"
        exit 1
    fi

    echo "Creating bucket: $bucket_name"
    aws s3api create-bucket --bucket "$bucket_name" --region "$BUCKET_REGION" --no-cli-pager

    echo "Removing public access block for $bucket_name"
    aws s3api delete-public-access-block --bucket "$bucket_name" --no-cli-pager

    echo "Setting bucket policy for $bucket_name"
    policy=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicRead",
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::$bucket_name/*"]
        }
    ]
}
EOF
)
    aws s3api put-bucket-policy --bucket "$bucket_name" --policy "$policy" --no-cli-pager

    echo "Configuring public access settings for $bucket_name"
    aws s3api put-public-access-block --bucket "$bucket_name" --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=false,RestrictPublicBuckets=false" --no-cli-pager

    echo "Bucket $bucket_name has been provisioned successfully."
}

create_user_and_access() {
    local username="sn12-validator-prompts-answers-user"
    echo "Creating IAM user: $username"
    aws iam create-user --user-name "$username" --no-cli-pager

    echo "Creating access key for $username"
    key_output=$(aws iam create-access-key --user-name "$username" --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text --no-cli-pager)
    access_key_id=$(echo "$key_output" | awk '{print $1}')
    secret_access_key=$(echo "$key_output" | awk '{print $2}')
    if [[ $access_key_id = "" ]]; then
        return 1
    fi

    echo "Creating IAM policy for bucket access"
    policy_document=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Statement1",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::$ANSWERS_BUCKET",
                "arn:aws:s3:::$ANSWERS_BUCKET/*",
                "arn:aws:s3:::$PROMPTS_BUCKET",
                "arn:aws:s3:::$PROMPTS_BUCKET/*"
            ]
        }
    ]
}
EOF
)

    policy_name="SN12ValidatorPromptsAnswersBucketsAccess"
    aws iam create-policy --policy-name "$policy_name" --policy-document "$policy_document" --no-cli-pager

    echo "Attaching policy to user"
    aws iam attach-user-policy --user-name "$username" --policy-arn "arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):policy/$policy_name" --no-cli-pager

    echo "User created successfully. Here are the access credentials:"
    echo "AWS_ACCESS_KEY_ID=$access_key_id"
    echo "AWS_SECRET_ACCESS_KEY=$secret_access_key"
    echo
    echo "Add these credentials in your validator .env file. You won't be able to retrieve the secret key again."
}

create_bucket "$PROMPTS_BUCKET"
create_bucket "$ANSWERS_BUCKET"

if $CREATE_USER; then
    create_user_and_access || echo "Failed to generate access key for IAM user!"
fi

echo "Add the bucket info in your validator .env file:"
echo "S3_BUCKET_NAME_PROMPTS=$PROMPTS_BUCKET"
echo "S3_BUCKET_NAME_ANSWERS=$ANSWERS_BUCKET"
