# Generated by Django 4.2.9 on 2024-01-28 11:03

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("miner", "0002_alter_acceptedjob_result_reported_to_validator_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="validator",
            name="active",
            field=models.BooleanField(default=True),
            preserve_default=False,
        ),
    ]
