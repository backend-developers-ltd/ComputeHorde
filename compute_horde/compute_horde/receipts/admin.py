from django.contrib import admin  # noqa

from compute_horde.base.admin import ReadOnlyAdminMixin
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt


class JobStartedReceiptsReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = [
        "job_uuid",
        "miner_hotkey",
        "validator_hotkey",
        "executor_class",
        "time_accepted",
        "max_timeout",
    ]
    ordering = ["-time_accepted"]


class JobFinishedReceiptsReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = [
        "job_uuid",
        "miner_hotkey",
        "validator_hotkey",
        "score",
        "time_started",
        "time_took",
    ]
    ordering = ["-time_started"]


admin.site.register(JobStartedReceipt, admin_class=JobStartedReceiptsReadOnlyAdmin)
admin.site.register(JobFinishedReceipt, admin_class=JobFinishedReceiptsReadOnlyAdmin)
