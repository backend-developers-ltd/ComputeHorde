from compute_horde.base.admin import AddOnlyAdminMixin, ReadOnlyAdminMixin
from compute_horde.executor_class import EXECUTOR_CLASS
from django import forms
from django.contrib import (
    admin,  # noqa
    messages,  # noqa
)
from django.utils.safestring import mark_safe  # noqa
from rangefilter.filters import DateTimeRangeFilter

from compute_horde_validator.validator.models import (
    AdminJobRequest,
    Miner,
    MinerBlacklist,
    OrganicJob,
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
    SyntheticJob,
    SystemEvent,
    ValidatorWhitelist,
    Weights,
)

# noqa
from compute_horde_validator.validator.tasks import trigger_run_admin_job_request  # noqa

admin.site.site_header = "ComputeHorde Validator Administration"
admin.site.site_title = "compute_horde_validator"
admin.site.index_title = "Welcome to ComputeHorde Validator Administration"

admin.site.index_template = "admin/validator_index.html"


class AdminJobRequestForm(forms.ModelForm):
    executor_class = forms.ChoiceField()

    class Meta:
        model = AdminJobRequest
        fields = [
            "uuid",
            "miner",
            "executor_class",
            "docker_image",
            "timeout",
            "args",
            "use_gpu",
            "input_url",
            "output_url",
            "status_message",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.fields:
            # exclude blacklisted miners from valid results
            self.fields["miner"].queryset = Miner.objects.exclude(minerblacklist__isnull=False)
            self.fields["executor_class"].choices = [(name, name) for name in EXECUTOR_CLASS]


class AdminJobRequestAddOnlyAdmin(admin.ModelAdmin, AddOnlyAdminMixin):
    form = AdminJobRequestForm
    exclude = ["env"]  # not used ?
    list_display = ["uuid", "executor_class", "docker_image", "use_gpu", "miner", "created_at"]
    readonly_fields = ["uuid", "status_message"]
    ordering = ["-created_at"]
    autocomplete_fields = ["miner"]

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        trigger_run_admin_job_request.delay(obj.id)
        organic_job = OrganicJob.objects.filter(job_uuid=obj.uuid).first()
        msg = (
            f"Please see <a href='/admin/validator/organicjob/{organic_job.pk}/change/'>ORGANIC JOB</a> for further details"
            if organic_job
            else f"Job {obj.uuid} failed to initialize"
        )
        messages.add_message(request, messages.INFO, mark_safe(msg))


class JobReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = ["job_uuid", "miner", "executor_class", "status", "updated_at"]
    search_fields = ["job_uuid", "miner__hotkey"]
    ordering = ["-updated_at"]


class MinerReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
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


class SystemEventAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = ["type", "subtype", "timestamp"]
    list_filter = ["type", "subtype", ("timestamp", DateTimeRangeFilter)]
    ordering = ["-timestamp"]


class WeightsReadOnlyAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = ["block", "created_at", "revealed_at"]
    ordering = ["-created_at"]


class PromptSeriesAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = [
        "series_uuid",
        "s3_url",
        "created_at",
        "generator_version",
    ]


class SolveWorkloadAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = [
        "workload_uuid",
        "seed",
        "s3_url",
        "created_at",
        "finished_at",
    ]


class PromptSampleAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = [
        "pk",
        "series",
        "workload",
        "synthetic_job",
        "created_at",
    ]


class PromptAdmin(admin.ModelAdmin, ReadOnlyAdminMixin):
    list_display = [
        "pk",
        "sample",
    ]


class ValidatorWhitelistAdmin(admin.ModelAdmin):
    list_display = [
        "hotkey",
        "created_at",
    ]


class MinerBlacklistAdmin(admin.ModelAdmin):
    list_display = ["miner_hotkey", "reason", "expires_at"]
    list_filter = ["reason"]
    ordering = ["-expires_at"]
    search_fields = ["miner__hotkey", "reason_details"]

    def miner_hotkey(self, obj):
        return obj.miner.hotkey

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("miner")


admin.site.register(Miner, admin_class=MinerReadOnlyAdmin)
admin.site.register(SyntheticJob, admin_class=JobReadOnlyAdmin)
admin.site.register(OrganicJob, admin_class=JobReadOnlyAdmin)
admin.site.register(MinerBlacklist, admin_class=MinerBlacklistAdmin)
admin.site.register(AdminJobRequest, admin_class=AdminJobRequestAddOnlyAdmin)
admin.site.register(SystemEvent, admin_class=SystemEventAdmin)
admin.site.register(Weights, admin_class=WeightsReadOnlyAdmin)
admin.site.register(PromptSeries, admin_class=PromptSeriesAdmin)
admin.site.register(SolveWorkload, admin_class=SolveWorkloadAdmin)
admin.site.register(PromptSample, admin_class=PromptSampleAdmin)
admin.site.register(Prompt, admin_class=PromptAdmin)
admin.site.register(ValidatorWhitelist, admin_class=ValidatorWhitelistAdmin)
