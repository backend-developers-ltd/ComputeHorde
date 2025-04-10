from compute_horde.executor_class import EXECUTOR_CLASS
from django import forms
from django.contrib import admin, messages
from django.contrib.admin import register
from django.db.models import QuerySet
from django.http.request import HttpRequest
from django.utils.safestring import mark_safe

from .models import (
    GPU,
    Channel,
    HotkeyWhitelist,
    Job,
    JobFeedback,
    JobStatus,
    Miner,
    MinerVersion,
    SignatureInfo,
    UserPreferences,
    Validator,
)

admin.site.site_header = "Facilitator Administration"
admin.site.site_title = "project"
admin.site.index_title = "Welcome to Facilitator Administration"


@register(GPU)
class GPUAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "capacity",
        "memory_type",
        "bus_width",
        "core_clock",
        "memory_clock",
        "fp16",
        "fp32",
        "fp64",
    )
    search_fields = ("name",)
    ordering = ("-capacity",)


@register(Channel)
class ChannelAdmin(admin.ModelAdmin):
    list_display = (
        "pk",
        "validator",
        "name",
        "last_heartbeat",
    )
    search_fields = (
        "validator__ss58_address",
        "name",
    )
    ordering = ("-pk",)
    autocomplete_fields = ("validator",)


@register(Validator)
class ValidatorAdmin(admin.ModelAdmin):
    list_display = (
        "pk",
        "ss58_address",
        "is_active",
        "last_job_time",
        "version",
        "runner_version",
    )
    list_filter = ("is_active",)
    search_fields = ("ss58_address",)
    ordering = (
        "-is_active",
        "-pk",
    )

    def get_queryset(self, request: HttpRequest) -> QuerySet[Validator]:
        return super().get_queryset(request).with_last_job_time()

    def last_job_time(self, obj: Validator) -> str:
        return obj.last_job_time.isoformat() if obj.last_job_time else "-"


@register(Miner)
class MinerAdmin(admin.ModelAdmin):
    list_display = (
        "pk",
        "ss58_address",
        "is_active",
    )
    list_filter = ("is_active",)
    search_fields = ("ss58_address",)
    ordering = (
        "-is_active",
        "-pk",
    )


@register(MinerVersion)
class MinerVersionAdmin(admin.ModelAdmin):
    list_display = (
        "pk",
        "miner",
        "version",
        "runner_version",
        "created_at",
    )
    search_fields = ("miner__ss58_address",)
    ordering = ("-created_at",)


@register(UserPreferences)
class UserPreferencesAdmin(admin.ModelAdmin):
    list_display = (
        "pk",
        "user",
        "get_validators_display",
        "get_miners_display",
        "exclusive",
    )
    search_fields = (
        "user__username",
        "user__email",
        "validators__ss58_address",
        "miners__ss58_address",
    )
    autocomplete_fields = (
        "user",
        "validators",
        "miners",
    )
    ordering = ("user__username",)

    def get_queryset(self, request: HttpRequest) -> QuerySet[UserPreferences]:
        return super().get_queryset(request).select_related("user").prefetch_related("validators", "miners")

    @admin.display(description="Validators")
    @mark_safe
    def get_validators_display(self, obj: UserPreferences) -> str:
        return "<ul>" + "".join(f"<li>{validator.ss58_address}</li>" for validator in obj.validators.all()) + "</ul>"

    @admin.display(description="Miners")
    @mark_safe
    def get_miners_display(self, obj: UserPreferences) -> str:
        return "<ul>" + "".join(f"<li>{miner.ss58_address}</li>" for miner in obj.miners.all()) + "</ul>"


@register(JobStatus)
class JobStatusAdmin(admin.ModelAdmin):
    list_display = (
        "pk",
        "job",
        "status",
        "created_at",
    )
    list_filter = (
        "status",
        "created_at",
    )
    search_fields = (
        "job__user__username",
        "job__user__email",
        "job__docker_image",
        "job__input_url",
        "job__output_upload_url",
        "job__output_download_url",
    )
    autocomplete_fields = ("job",)
    ordering = ("-created_at",)

    def get_queryset(self, request: HttpRequest) -> QuerySet[JobStatus]:
        return super().get_queryset(request).select_related("job__user")


class JobStatusInline(admin.TabularInline):
    model = JobStatus
    extra = 0
    ordering = ("created_at",)


class JobAdminForm(forms.ModelForm):
    executor_class = forms.ChoiceField()

    class Meta:
        model = Job
        fields = [
            "user",
            "validator",
            "miner",
            "created_at",
            "executor_class",
            "docker_image",
            "args",
            "env",
            "use_gpu",
            "input_url",
            "output_download_url_expires_at",
            "tag",
            "output_upload_url",
            "output_download_url",
        ]

    def __init__(self, *args, **kwargs):
        super(__class__, self).__init__(*args, **kwargs)
        if self.fields:
            self.fields["executor_class"].choices = [(name, name) for name in EXECUTOR_CLASS]


@register(Job)
class JobAdmin(admin.ModelAdmin):
    form = JobAdminForm
    list_display = (
        "pk",
        "user",
        "executor_class",
        "docker_image",
        "created_at",
        "status",
        "get_elapsed_display",
    )
    list_filter = (
        "created_at",
        "statuses__status",
    )
    search_fields = (
        "user__username",
        "user__email",
        "docker_image",
        "input_url",
        "output_upload_url",
        "output_download_url",
    )
    ordering = ("-created_at",)
    autocomplete_fields = (
        "user",
        "validator",
    )
    readonly_fields = (
        "output_upload_url",
        "output_download_url",
    )
    inlines = [JobStatusInline]

    def get_queryset(self, request: HttpRequest) -> QuerySet[Job]:
        return super().get_queryset(request).with_statuses()

    def status(self, obj: Job) -> str:
        return list(obj.statuses.all())[-1].get_status_display()

    def save_model(self, request, obj, form, change):
        try:
            super().save_model(request, obj, form, change)
        except Miner.DoesNotExist:
            self.message_user(request, "No active miners available", level=messages.ERROR)
        except Validator.DoesNotExist:
            self.message_user(request, "No active validators available", level=messages.ERROR)

    def get_elapsed_display(self, obj: Job) -> str | None:
        if elapsed := obj.elapsed:
            return str(elapsed).split(".")[0]


@register(JobFeedback)
class JobFeedbackAdmin(admin.ModelAdmin):
    list_display = (
        "job",
        "user",
        "created_at",
        "result_correctness",
        "expected_duration",
        "signature_info__signature_type",
    )
    search_fields = ("=job__uuid", "^user__username")
    list_filter = ("created_at", "result_correctness")

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        queryset = queryset.select_related("job", "user", "signature_info")
        return queryset

    def signature_info__signature_type(self, obj):
        return obj.signature_info.signature_type


@register(HotkeyWhitelist)
class HotkeyWhitelistAdmin(admin.ModelAdmin):
    list_display = ("ss58_address",)
    search_fields = ("ss58_address",)


# TODO: deprecate
@admin.register(SignatureInfo)
class SignatureInfoAdmin(admin.ModelAdmin):
    list_display = ("timestamp_ns", "signature_type", "signatory", "signed_payload")
    search_fields = ("=signature_type", "^signatory")
    list_filter = ("signature_type",)
