from django.contrib import admin  # noqa

from compute_horde.base.admin import ReadOnlyAdminMixin
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt, JobAcceptedReceipt


class JobStartedReceiptsReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = [
        "job_uuid",
        "miner_hotkey",
        "validator_hotkey",
        "timestamp",
        "executor_class",
        "ttl",
    ]
    ordering = ["-timestamp"]


class JobAcceptedReceiptsReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = [
        "job_uuid",
        "miner_hotkey",
        "validator_hotkey",
        "timestamp",
        "time_accepted",
        "ttl",
    ]
    ordering = ["-timestamp"]


class JobFinishedReceiptsReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = [
        "job_uuid",
        "miner_hotkey",
        "validator_hotkey",
        "timestamp",
        "score",
        "time_started",
        "time_took",
    ]
    ordering = ["-timestamp"]


admin.site.register(JobStartedReceipt, admin_class=JobStartedReceiptsReadOnlyAdmin)
admin.site.register(JobAcceptedReceipt, admin_class=JobAcceptedReceiptsReadOnlyAdmin)
admin.site.register(JobFinishedReceipt, admin_class=JobFinishedReceiptsReadOnlyAdmin)
