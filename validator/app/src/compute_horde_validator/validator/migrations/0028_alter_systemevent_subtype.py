# Generated by Django 4.2.13 on 2024-08-06 12:33

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("validator", "0027_weights_weights_unique_block"),
    ]

    operations = [
        migrations.AlterField(
            model_name="systemevent",
            name="subtype",
            field=models.CharField(
                choices=[
                    ("SUCCESS", "Success"),
                    ("FAILURE", "Failure"),
                    ("SUBTENSOR_CONNECTIVITY_ERROR", "Subtensor Connectivity Error"),
                    (
                        "SUBTENSOR_HYPERPARAMETERS_FETCH_ERROR",
                        "Subtensor Hyperparameters Fetch Error",
                    ),
                    ("COMMIT_WEIGHTS_SUCCESS", "Commit Weights Success"),
                    ("COMMIT_WEIGHTS_ERROR", "Commit Weights Error"),
                    ("COMMIT_WEIGHTS_UNREVEALED_ERROR", "Commit Weights Unrevealed Error"),
                    ("REVEAL_WEIGHTS_ERROR", "Reveal Weights Error"),
                    ("REVEAL_WEIGHTS_SUCCESS", "Reveal Weights Success"),
                    ("SET_WEIGHTS_SUCCESS", "Set Weights Success"),
                    ("SET_WEIGHTS_ERROR", "Set Weights Error"),
                    ("GENERIC_ERROR", "Generic Error"),
                    ("WRITING_TO_CHAIN_TIMEOUT", "Writing To Chain Timeout"),
                    ("WRITING_TO_CHAIN_FAILED", "Writing To Chain Failed"),
                    ("WRITING_TO_CHAIN_GENERIC_ERROR", "Writing To Chain Generic Error"),
                    ("GIVING_UP", "Giving Up"),
                    ("MANIFEST_ERROR", "Manifest Error"),
                    ("MANIFEST_TIMEOUT", "Manifest Timeout"),
                    ("MINER_CONNECTION_ERROR", "Miner Connection Error"),
                    ("JOB_NOT_STARTED", "Job Not Started"),
                    ("JOB_REJECTED", "Job Rejected"),
                    ("JOB_EXECUTION_TIMEOUT", "Job Execution Timeout"),
                    ("RECEIPT_FETCH_ERROR", "Receipt Fetch Error"),
                    ("RECEIPT_SEND_ERROR", "Receipt Send Error"),
                    ("SPECS_SENDING_ERROR", "Specs Send Error"),
                    ("HEARTBEAT_ERROR", "Heartbeat Error"),
                    ("UNEXPECTED_MESSAGE", "Unexpected Message"),
                    ("UNAUTHORIZED", "Unauthorized"),
                ],
                max_length=255,
            ),
        ),
    ]
