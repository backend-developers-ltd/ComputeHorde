# Generated by Django 4.2.19 on 2025-02-25 19:20

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0038_alter_job_args"),
    ]

    operations = [
        migrations.AddField(
            model_name="job",
            name="on_trusted_miner",
            field=models.BooleanField(default=False),
        ),
    ]
