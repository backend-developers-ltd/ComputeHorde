# Generated by Django 4.2.13 on 2024-07-03 09:27

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("miner", "0007_jobstartedreceipt"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="JobReceipt",
            new_name="JobFinishedReceipt",
        ),
    ]
