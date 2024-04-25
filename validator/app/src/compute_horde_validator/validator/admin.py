from django.contrib import admin  # noqa
from django.contrib.admin import register  # noqa
from django.contrib.auth.models import User  # noqa

from compute_horde_validator.validator.models import Miner, OrganicJob, SyntheticJob


admin.site.site_header = "compute_horde_validator Administration"
admin.site.site_title = "compute_horde_validator"
admin.site.index_title = "Welcome to compute_horde_validator Administration"

class AddOnlyAdmin(admin.ModelAdmin):
    def has_change_permission(self, *args, **kwargs):
        return False

    def has_delete_permission(self, *args, **kwargs):
        return False

class JobAddOnlyAdmin(AddOnlyAdmin):
    search_fields = ['job_uuid']

class MinerReadOnlyAdmin(AddOnlyAdmin):
    change_form_template = "admin/read_only_view.html"
    search_fields = ['hotkey']

    def has_add_permission(self, *args, **kwargs):
        return False

admin.site.register(Miner, admin_class=MinerReadOnlyAdmin)
admin.site.register(SyntheticJob, admin_class=JobAddOnlyAdmin)
admin.site.register(OrganicJob, admin_class=JobAddOnlyAdmin)
