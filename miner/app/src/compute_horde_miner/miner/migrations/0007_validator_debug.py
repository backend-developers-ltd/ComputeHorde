# Generated by Django 4.2.13 on 2024-07-08 21:17

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("miner", "0006_remove_jobreceipt_job"),
    ]

    operations = [
        migrations.AddField(
            model_name="validator",
            name="debug",
            field=models.BooleanField(default=False),
        ),
    ]
