# Generated by Django 5.0.4 on 2024-05-30 13:27

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("validator", "0009_remove_syntheticjob_one_job_per_batch_per_miner"),
    ]

    operations = [
        migrations.CreateModel(
            name="JobReceipt",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("job_uuid", models.UUIDField()),
                ("miner_hotkey", models.CharField(max_length=256)),
                ("validator_hotkey", models.CharField(max_length=256)),
                ("time_started", models.DateTimeField()),
                ("time_took_us", models.BigIntegerField()),
                ("score_str", models.CharField(max_length=256)),
            ],
        ),
        migrations.AddConstraint(
            model_name="jobreceipt",
            constraint=models.UniqueConstraint(
                fields=("job_uuid",), name="unique_job_receipt_job_uuid"
            ),
        ),
    ]