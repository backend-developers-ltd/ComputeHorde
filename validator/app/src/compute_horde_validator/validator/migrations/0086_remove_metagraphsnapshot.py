from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("validator", "0085_weightsettingfinishedevent_and_more"),
    ]

    operations = [
        migrations.DeleteModel(
            name="MetagraphSnapshot",
        ),
    ]
