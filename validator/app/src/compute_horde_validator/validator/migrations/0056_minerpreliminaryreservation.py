# Generated by Django 4.2.19 on 2025-03-03 20:36

import uuid

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("validator", "0055_organicjob_on_trusted_miner"),
    ]

    operations = [
        migrations.CreateModel(
            name="MinerPreliminaryReservation",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("executor_class", models.CharField(max_length=255)),
                ("job_uuid", models.UUIDField(default=uuid.uuid4, unique=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("expires_at", models.DateTimeField()),
                (
                    "miner",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE, to="validator.miner"
                    ),
                ),
            ],
        ),
    ]
