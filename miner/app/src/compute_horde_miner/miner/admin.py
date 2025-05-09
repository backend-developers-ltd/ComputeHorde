from django.contrib import admin  # noqa
from django.contrib.admin import register, AdminSite  # noqa

from compute_horde.base.admin import ReadOnlyAdminMixin
from compute_horde_miner.miner.models import (
    AcceptedJob,
    Validator,
    ValidatorBlacklist,
)


admin.site.site_header = "ComputeHorde Miner Administration"
admin.site.site_title = "compute_horde_miner"
admin.site.index_title = "Welcome to ComputeHorde Miner Administration"

admin.site.index_template = "admin/miner_index.html"


class ValidatorReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    search_fields = ["public_key"]


class AcceptedJobReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
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


class JobStartedReceiptsReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = [
        "job_uuid",
        "validator_hotkey",
        "executor_class",
        "time_accepted",
    ]
    ordering = ["-time_accepted"]


class JobFinishedReceiptsReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = ["job_uuid", "validator_hotkey", "score", "time_started", "time_took"]
    ordering = ["-time_started"]


admin.site.register(AcceptedJob, admin_class=AcceptedJobReadOnlyAdmin)
admin.site.register(Validator, admin_class=ValidatorReadOnlyAdmin)
admin.site.register(ValidatorBlacklist)
