# Generated by Django 4.2.13 on 2024-07-02 16:08

import compute_horde_core.executor_class
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0022_signatureinfo_jobfeedback"),
    ]

    operations = [
        migrations.AddField(
            model_name="job",
            name="executor_class",
            field=models.CharField(
                default=compute_horde_core.executor_class.ExecutorClass["spin_up_4min__gpu_24gb"],
                help_text="executor hardware class",
                max_length=255,
            ),
        ),
        migrations.AddField(
            model_name="jobreceipt",
            name="executor_class",
            field=models.CharField(
                default=compute_horde_core.executor_class.ExecutorClass["spin_up_4min__gpu_24gb"], max_length=255
            ),
        ),
    ]
