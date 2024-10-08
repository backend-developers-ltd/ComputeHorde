#!/bin/bash
set -eu -o pipefail

if ! command -v aws &> /dev/null; then
    echo "AWS CLI cannot be found. Please, install and configure it according to the instructions for your OS: https://aws.amazon.com/cli/"
    exit 1
fi

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <prompts_bucket_name> <answers_bucket_name> [--create-user]"
    exit 1
fi

PROMPTS_BUCKET=$1
ANSWERS_BUCKET=$2
CREATE_USER=false

if [ "$#" -eq 3 ] && [ "$3" == "--create-user" ]; then
    CREATE_USER=true
fi

BUCKET_REGION="${AWS_REGION:-us-east-1}"

create_bucket() {
    local bucket_name=$1

    if aws s3api head-bucket --bucket "$bucket_name" --no-cli-pager 2>/dev/null; then
        echo "Bucket $bucket_name already exists and is owned by you. Skipping creation."
        return
    elif [ $? -eq 254 ]; then
        echo "Bucket $bucket_name already exists but is not owned by you. Exiting."
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
    echo "Access Key ID: $access_key_id"
    echo "Secret Access Key: $secret_access_key"
    echo "Please save these credentials securely. You won't be able to retrieve the secret key again."
}

create_bucket "$PROMPTS_BUCKET"
create_bucket "$ANSWERS_BUCKET"

if $CREATE_USER; then
    create_user_and_access
fi
