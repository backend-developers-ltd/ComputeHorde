# Generated manually to delete MinerManifest records with batch=None

from django.db import migrations


def delete_miner_manifests_with_null_batch(apps, schema_editor):
    """
    Delete all MinerManifest records where batch is None.
    """
    MinerManifest = apps.get_model("validator", "MinerManifest")
    deleted_count = MinerManifest.objects.filter(batch__isnull=True).delete()[0]
    print(f"Deleted {deleted_count} MinerManifest records with batch=None")


def reverse_delete_miner_manifests_with_null_batch(apps, schema_editor):
    """
    This migration cannot be reversed as it deletes data.
    """
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("validator", "0069_alter_minermanifest_batch_and_more"),
    ]

    operations = [
        migrations.RunPython(
            delete_miner_manifests_with_null_batch,
            reverse_delete_miner_manifests_with_null_batch,
        ),
    ]
