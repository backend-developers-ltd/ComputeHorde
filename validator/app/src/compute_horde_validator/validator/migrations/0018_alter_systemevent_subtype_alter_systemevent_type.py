# Generated by Django 4.2.13 on 2024-06-27 11:09

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("validator", "0017_adminjobrequest_executor_class"),
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
                    ("GENERIC_ERROR", "Generic Error"),
                    ("WRITING_TO_CHAIN_TIMEOUT", "Writing To Chain Timeout"),
                    ("WRITING_TO_CHAIN_FAILED", "Writing To Chain Failed"),
                    (
                        "WRITING_TO_CHAIN_GENERIC_ERROR",
                        "Writing To Chain Generic Error",
                    ),
                    ("GIVING_UP", "Giving Up"),
                    ("MANIFEST_ERROR", "Manifest Error"),
                    ("MINER_CONNECTION_ERROR", "Miner Connection Error"),
                    ("JOB_NOT_STARTED", "Job Not Started"),
                    ("JOB_EXECUTION_TIMEOUT", "Job Execution Timeout"),
                    ("RECEIPT_FETCH_ERROR", "Receipt Fetch Error"),
                    ("RECEIPT_SEND_ERROR", "Receipt Send Error"),
                    ("SPECS_SENDING_ERROR", "Specs Send Error"),
                    ("HEARTBEAT_ERROR", "Heartbeat Error"),
                ],
                max_length=255,
            ),
        ),
        migrations.AlterField(
            model_name="systemevent",
            name="type",
            field=models.CharField(
                choices=[
                    ("WEIGHT_SETTING_SUCCESS", "Weight Setting Success"),
                    ("WEIGHT_SETTING_FAILURE", "Weight Setting Failure"),
                    ("MINER_ORGANIC_JOB_FAILURE", "Miner Organic Job Failure"),
                    ("MINER_ORGANIC_JOB_SUCCESS", "Miner Organic Job Success"),
                    ("MINER_SYNTHETIC_JOB_SUCCESS", "Miner Synthetic Job Success"),
                    ("MINER_SYNTHETIC_JOB_FAILURE", "Miner Synthetic Job Failure"),
                    ("RECEIPT_FAILURE", "Receipt Failure"),
                    ("FACILITATOR_CLIENT_ERROR", "Facilitator Client Error"),
                ],
                max_length=255,
            ),
        ),
    ]
