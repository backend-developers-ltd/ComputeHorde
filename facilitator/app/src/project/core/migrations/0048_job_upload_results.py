# Generated by Django 4.2.19 on 2025-04-16 21:54

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0047_remove_job_signature_info_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="job",
            name="upload_results",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
