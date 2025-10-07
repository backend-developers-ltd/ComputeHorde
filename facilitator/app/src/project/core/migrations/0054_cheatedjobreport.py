from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0053_job_job_namespace"),
    ]

    operations = [
        migrations.CreateModel(
            name="CheatedJobReport",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("details", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "job",
                    models.OneToOneField(
                        on_delete=models.CASCADE,
                        related_name="cheated_report",
                        to="core.job",
                    ),
                ),
            ],
        ),
    ]
