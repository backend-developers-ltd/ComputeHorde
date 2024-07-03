from django.contrib import admin  # noqa
from django.contrib.admin import register, AdminSite  # noqa

from compute_horde_miner.miner.models import (
    AcceptedJob,
    Validator,
    ValidatorBlacklist,
    JobFinishedReceipt,
)


admin.site.site_header = "ComputeHorde Miner Administration"
admin.site.site_title = "compute_horde_miner"
admin.site.index_title = "Welcome to ComputeHorde Miner Administration"

admin.site.index_template = "admin/miner_index.html"


class ReadOnlyAdmin(admin.ModelAdmin):
    change_form_template = "admin/read_only_view.html"

    def has_add_permission(self, *args, **kwargs):
        return False

    def has_change_permission(self, *args, **kwargs):
        return False

    def has_delete_permission(self, *args, **kwargs):
        return False


class ValidatorReadOnlyAdmin(ReadOnlyAdmin):
    search_fields = ["public_key"]


class AcceptedJobReadOnlyAdmin(ReadOnlyAdmin):
    list_display = [
        "job_uuid",
        "validator",
        "status",
        "result_reported_to_validator",
        "created_at",
        "updated_at",
    ]
    search_fields = ["job_uuid", "validator__public_key"]
    ordering = ["-created_at"]


class JobFinishedReceiptsReadOnlyAdmin(ReadOnlyAdmin):
    list_display = ["job_uuid", "validator_hotkey", "score", "time_started", "time_took"]
    ordering = ["-time_started"]


admin.site.register(AcceptedJob, admin_class=AcceptedJobReadOnlyAdmin)
admin.site.register(Validator, admin_class=ValidatorReadOnlyAdmin)
admin.site.register(JobFinishedReceipt, admin_class=JobFinishedReceiptsReadOnlyAdmin)
admin.site.register(ValidatorBlacklist)
