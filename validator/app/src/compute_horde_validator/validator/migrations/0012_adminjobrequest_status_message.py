# Generated by Django 4.2.11 on 2024-06-04 12:18

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("validator", "0011_organicjob_stderr_organicjob_stdout"),
    ]

    operations = [
        migrations.AddField(
            model_name="adminjobrequest",
            name="status_message",
            field=models.TextField(blank=True, default=""),
        ),
    ]
