# Generated by Django 5.0.4 on 2024-05-29 07:11

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("miner", "0005_alter_validatorblacklist_options_acceptedjob_score_and_more"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="jobreceipt",
            name="job",
        ),
    ]