import pydantic
from compute_horde_core.output_upload import SingleFileUpload
from django.db import migrations
from structlog import get_logger

log = get_logger(__name__)

UPLOADS_ADAPTER = pydantic.TypeAdapter(list[SingleFileUpload])


def is_valid_uploads(uploads) -> bool:
    # The field hands back the raw JSON string when its own validation fails.
    try:
        if isinstance(uploads, str):
            UPLOADS_ADAPTER.validate_json(uploads)
        else:
            UPLOADS_ADAPTER.validate_python(uploads)
    except pydantic.ValidationError:
        return False
    return True


def clear_malformed_uploads(apps, schema_editor):
    """
    Empty Job.uploads where it no longer validates, e.g. an absolute relative_path stored
    before such paths were rejected, since loading such a job raises ValidationError.
    """
    Job = apps.get_model("core", "Job")
    malformed = [
        uuid for uuid, uploads in Job.objects.values_list("uuid", "uploads").iterator() if not is_valid_uploads(uploads)
    ]
    if malformed:
        Job.objects.filter(uuid__in=malformed).update(uploads=[])
        log.warning("Cleared job uploads that fail validation", job_uuids=[str(uuid) for uuid in malformed])


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0054_cheatedjobreport"),
    ]

    operations = [
        migrations.RunPython(clear_malformed_uploads, migrations.RunPython.noop),
    ]
