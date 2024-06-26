# Generated by Django 4.2.9 on 2024-01-21 22:45

import django.db.models.deletion
from django.db import migrations, models

import compute_horde_miner.miner.models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Validator",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("public_key", models.TextField(unique=True)),
            ],
        ),
        migrations.CreateModel(
            name="AcceptedJob",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("job_uuid", models.UUIDField()),
                ("executor_token", models.CharField(max_length=73)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("WAITING_FOR_EXECUTOR", "Waiting For Executor"),
                            ("WAITING_FOR_PAYLOAD", "Waiting For Payload"),
                            ("RUNNING", "Running"),
                            ("FINISHED", "Finished"),
                            ("FAILED", "Failed"),
                        ],
                        max_length=20,
                    ),
                ),
                (
                    "initial_job_details",
                    models.JSONField(encoder=compute_horde_miner.miner.models.EnumEncoder),
                ),
                (
                    "full_job_details",
                    models.JSONField(
                        encoder=compute_horde_miner.miner.models.EnumEncoder, null=True
                    ),
                ),
                ("exit_status", models.PositiveSmallIntegerField(null=True)),
                ("stdout", models.TextField(blank=True, default="")),
                ("stderr", models.TextField(blank=True, default="")),
                ("result_reported_to_validator", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "validator",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE, to="miner.validator"
                    ),
                ),
            ],
        ),
    ]
