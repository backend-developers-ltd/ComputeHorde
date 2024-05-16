from django.contrib import admin  # noqa
from django.contrib.admin import register  # noqa
from django.contrib.auth.models import User  # noqa

from compute_horde_validator.validator.models import Miner, MinerBlacklist, OrganicJob, SyntheticJob, MinerBlacklist, AdminJobRequest  # noqa


admin.site.site_header = "ComputeHorde Validator Administration"
admin.site.site_title = "compute_horde_validator"
admin.site.index_title = "Welcome to ComputeHorde Validator Administration"

admin.site.index_template = "admin/validator_index.html"


class AddOnlyAdmin(admin.ModelAdmin):
    def has_change_permission(self, *args, **kwargs):
        return False

    def has_delete_permission(self, *args, **kwargs):
        return False

class AddOnlyAdminJobRequestAdmin(AddOnlyAdmin):
    list_display = ['miner', 'docker_image', 'args', 'created_at']
    ordering = ['-created_at']

class JobReadOnlyAdmin(AddOnlyAdmin):
    list_display = ['job_uuid', 'miner', 'status', 'updated_at']
    search_fields = ['job_uuid', 'miner__hotkey']
    ordering = ['-updated_at']

    def has_add_permission(self, *args, **kwargs):
        return False


class MinerReadOnlyAdmin(AddOnlyAdmin):
    change_form_template = "admin/read_only_view.html"
    search_fields = ["hotkey"]

    def has_add_permission(self, *args, **kwargs):
        return False


admin.site.register(Miner, admin_class=MinerReadOnlyAdmin)
admin.site.register(SyntheticJob, admin_class=JobReadOnlyAdmin)
admin.site.register(OrganicJob, admin_class=JobReadOnlyAdmin)
admin.site.register(MinerBlacklist)
admin.site.register(AdminJobRequest, admin_class=AddOnlyAdminJobRequestAdmin)
