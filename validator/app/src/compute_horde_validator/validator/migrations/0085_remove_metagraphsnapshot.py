from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("validator", "0084_merge_20250926_1128"),
    ]

    operations = [
        migrations.DeleteModel(
            name="MetagraphSnapshot",
        ),
    ]
