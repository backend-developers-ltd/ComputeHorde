from django.contrib import admin  # noqa
from django.shortcuts import redirect  # noqa
from django import forms

from compute_horde_validator.validator.models import (
    Miner,
    OrganicJob,
    SyntheticJob,
    MinerBlacklist,
    AdminJobRequest,
    JobReceipt,
)  # noqa

from compute_horde_validator.validator.tasks import trigger_run_admin_job_request  # noqa

admin.site.site_header = "ComputeHorde Validator Administration"
admin.site.site_title = "compute_horde_validator"
admin.site.index_title = "Welcome to ComputeHorde Validator Administration"

admin.site.index_template = "admin/validator_index.html"


class AddOnlyAdmin(admin.ModelAdmin):
    def has_change_permission(self, *args, **kwargs):
        return False

    def has_delete_permission(self, *args, **kwargs):
        return False


class ReadOnlyAdmin(AddOnlyAdmin):
    def has_add_permission(self, *args, **kwargs):
        return False


class AdminJobRequestForm(forms.ModelForm):
    class Meta:
        model = AdminJobRequest
        fields = [
            "uuid",
            "miner",
            "docker_image",
            "timeout",
            "raw_script",
            "args",
            "use_gpu",
            "input_url",
            "output_url",
        ]

    def __init__(self, *args, **kwargs):
        super(__class__, self).__init__(*args, **kwargs)
        if self.fields:
            # exclude blacklisted miners from valid results
            self.fields["miner"].queryset = Miner.objects.exclude(minerblacklist__isnull=False)


class AdminJobRequestAddOnlyAdmin(AddOnlyAdmin):
    form = AdminJobRequestForm
    exclude = ["env", "status_message"]  # not used ?
    list_display = ["uuid", "docker_image", "use_gpu", "miner", "created_at"]
    ordering = ["-created_at"]
    autocomplete_fields = ["miner"]

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        trigger_run_admin_job_request.delay(obj.id)

    def response_post_save_add(self, request, obj):
        # Redirect to the corresponding OrganicJob view
        organic_job = OrganicJob.objects.filter(job_uuid=obj.uuid).first()
        if organic_job:
            return redirect(organic_job)
        return super().response_post_save_add(request, obj)


class JobReadOnlyAdmin(ReadOnlyAdmin):
    list_display = ["job_uuid", "miner", "status", "updated_at"]
    search_fields = ["job_uuid", "miner__hotkey"]
    ordering = ["-updated_at"]


class MinerReadOnlyAdmin(ReadOnlyAdmin):
    change_form_template = "admin/read_only_view.html"
    search_fields = ["hotkey"]

    def has_add_permission(self, *args, **kwargs):
        return False

    # exclude blacklisted miners from autocomplete results
    def get_search_results(self, request, queryset, search_term):
        queryset, use_distinct = super().get_search_results(
            request,
            queryset,
            search_term,
        )
        queryset = queryset.exclude(minerblacklist__isnull=False)
        return queryset, use_distinct


class JobReceiptsReadOnlyAdmin(ReadOnlyAdmin):
    list_display = [
        "job_uuid",
        "miner_hotkey",
        "validator_hotkey",
        "score",
        "time_started",
        "time_took",
    ]
    ordering = ["-time_started"]


admin.site.register(Miner, admin_class=MinerReadOnlyAdmin)
admin.site.register(SyntheticJob, admin_class=JobReadOnlyAdmin)
admin.site.register(OrganicJob, admin_class=JobReadOnlyAdmin)
admin.site.register(JobReceipt, admin_class=JobReceiptsReadOnlyAdmin)
admin.site.register(MinerBlacklist)
admin.site.register(AdminJobRequest, admin_class=AdminJobRequestAddOnlyAdmin)
