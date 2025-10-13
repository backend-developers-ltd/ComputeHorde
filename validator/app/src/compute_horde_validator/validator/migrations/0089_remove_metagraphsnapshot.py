from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("validator", "0088_metagraphsnapshot"),
    ]

    operations = [
        migrations.DeleteModel(
            name="MetagraphSnapshot",
        ),
    ]
