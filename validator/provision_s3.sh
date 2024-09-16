#!/bin/bash
set -eu -o pipefail

if ! command -v aws &> /dev/null; then
    echo "AWS CLI cannot be found. Please, install and configure it according to the instructions for your OS: https://aws.amazon.com/cli/"
    exit 1
fi

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <prompts_bucket_name> <answers_bucket_name>"
    exit 1
fi

PROMPTS_BUCKET=$1
ANSWERS_BUCKET=$2

BUCKET_REGION="${AWS_REGION:-us-east-1}"

create_bucket() {
    local bucket_name=$1

    if aws s3api head-bucket --bucket "$bucket_name" --no-cli-pager 2>/dev/null; then
      echo "Bucket name $bucket_name is already taken. Please, choose a different one."
      exit 1
    fi
    
    echo "Creating bucket: $bucket_name"
    aws s3api create-bucket --bucket "$bucket_name" --region "$BUCKET_REGION" --no-cli-pager

    # aws puts a public access block by default, so we need to remove it
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

create_bucket "$PROMPTS_BUCKET"
create_bucket "$ANSWERS_BUCKET"
