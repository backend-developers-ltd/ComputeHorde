from django.contrib import admin  # noqa

from compute_horde.mv_protocol.models import JobFinishedReceipt, JobStartedReceipt

class ReadOnlyAdmin(admin.ModelAdmin):
    def has_change_permission(self, *args, **kwargs):
        return False

    def has_delete_permission(self, *args, **kwargs):
        return False

    def has_add_permission(self, *args, **kwargs):
        return False


class JobStartedReceiptsReadOnlyAdmin(ReadOnlyAdmin):
    list_display = [
        "job_uuid",
        "miner_hotkey",
        "validator_hotkey",
        "executor_class",
        "time_accepted",
        "max_timeout",
    ]
    ordering = ["-time_accepted"]


class JobFinishedReceiptsReadOnlyAdmin(ReadOnlyAdmin):
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
