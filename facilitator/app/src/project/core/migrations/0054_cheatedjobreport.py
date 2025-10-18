import django.db.models.deletion
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
                (
                    "trusted_job",
                    models.ForeignKey(
                        help_text="The trusted job this cheat report is filed against",
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="cheat_reports_against",
                        to="core.job",
                    ),
                ),
            ],
        ),
    ]
