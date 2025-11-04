from compute_horde.base.admin import AddOnlyAdminMixin, ReadOnlyAdminMixin
from compute_horde.executor_class import EXECUTOR_CLASS
from django import forms
from django.contrib import (
    admin,  # noqa
    messages,  # noqa
)
from django.contrib.admin import FieldListFilter
from django.db.models import Count, OuterRef, Subquery, Sum
from django.utils.safestring import mark_safe  # noqa
from rangefilter.filters import DateTimeRangeFilter, NumericRangeFilter

from compute_horde_validator.validator.models import (
    AdminJobRequest,
    Miner,
    MinerBlacklist,
    OrganicJob,
    SystemEvent,
    ValidatorWhitelist,
)
from compute_horde_validator.validator.models.allowance.internal import (
    AllowanceBooking,
    AllowanceMinerManifest,
    BlockAllowance,
)
from compute_horde_validator.validator.models.scoring.internal import Weights

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


class NotNullFilter(FieldListFilter):
    """
    Filter for a nullable field.
    Shows the following options:
    - All: Show all records
    - Present: Show records with the field not null
    - Empty: Show records with the field null
    """

    def choices(self, changelist):
        yield {
            "selected": self.lookup_val is None,
            "query_string": changelist.get_query_string(remove=[self.lookup_kwarg]),
            "display": "All",
        }
        yield {
            "selected": self.lookup_val == "yes",
            "query_string": changelist.get_query_string({self.lookup_kwarg: "yes"}),
            "display": "Present",
        }
        yield {
            "selected": self.lookup_val == "no",
            "query_string": changelist.get_query_string({self.lookup_kwarg: "no"}),
            "display": "Empty",
        }

    def queryset(self, request, queryset):
        if self.lookup_val == "yes":
            return queryset.filter(**{f"{self.field_path}__isnull": False})
        if self.lookup_val == "no":
            return queryset.filter(**{f"{self.field_path}__isnull": True})
        return queryset

    def expected_parameters(self):
        return [self.lookup_kwarg]

    @property
    def lookup_kwarg(self):
        return f"has_{self.field_path}"

    @property
    def lookup_val(self):
        return self.used_parameters.get(self.lookup_kwarg)


class AllowanceMinerManifestAdmin(ReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = [
        "block_number",
        "miner_ss58address",
        "executor_class",
        "executor_count",
        "success",
        "is_drop",
    ]
    list_filter = [
        "executor_class",
        "success",
        "is_drop",
        ("executor_count", NumericRangeFilter),
        ("block_number", NumericRangeFilter),
    ]
    search_fields = ["miner_ss58address"]
    ordering = ["-block_number", "miner_ss58address", "executor_class"]


class BlockAllowanceAdmin(ReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = [
        "block_number",
        "validator_ss58",
        "miner_ss58",
        "executor_class",
        "allowance",
        "is_booked",
        "is_invalidated",
    ]
    list_filter = [
        "executor_class",
        ("block_id", NumericRangeFilter),
        ("allowance", NumericRangeFilter),
        ("allowance_booking", NotNullFilter),
        ("invalidated_at_block", NotNullFilter),
    ]
    search_fields = ["validator_ss58", "miner_ss58"]
    ordering = ["-block_id", "validator_ss58", "miner_ss58", "executor_class"]

    def block_number(self, obj):
        # Need to "alias" block_id as block_number or django will have trouble with sorting by an implicit field
        return obj.block_id

    block_number.admin_order_field = "block_id"
    block_number.short_description = "Block"

    def is_booked(self, obj):
        return obj.allowance_booking is not None

    is_booked.boolean = True

    def is_invalidated(self, obj):
        return obj.invalidated_at_block is not None

    is_invalidated.boolean = True


class BlockAllowanceInline(ReadOnlyAdminMixin, admin.TabularInline):
    model = BlockAllowance
    fields = [
        "block_number",
        "validator_ss58",
        "miner_ss58",
        "executor_class",
        "allowance",
        "invalidated_at_block",
    ]
    readonly_fields = ["block_number"]

    def block_number(self, obj):
        # Need to "alias" block_id as block_number or django will not see this field at all
        return obj.block_id

    block_number.short_description = "Block"


class AllowanceBookingAdmin(ReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = [
        "id",
        "creation_timestamp",
        "validator",
        "miner",
        "total_allowance",
        "is_reserved",
        "is_spent",
        "block_count",
        "reservation_expiry_time",
    ]
    list_filter = [
        "is_reserved",
        "is_spent",
        ("creation_timestamp", DateTimeRangeFilter),
        ("reservation_expiry_time", DateTimeRangeFilter),
    ]
    search_fields = ["blockallowance__validator_ss58", "blockallowance__miner_ss58"]
    ordering = ["-creation_timestamp"]
    inlines = [BlockAllowanceInline]

    def get_queryset(self, request):
        first_block_validator = Subquery(
            BlockAllowance.objects.filter(allowance_booking=OuterRef("pk")).values(
                "validator_ss58"
            )[:1]
        )
        first_block_miner = Subquery(
            BlockAllowance.objects.filter(allowance_booking=OuterRef("pk")).values("miner_ss58")[:1]
        )

        return (
            super()
            .get_queryset(request)
            .annotate(
                block_count=Count("blockallowance"),
                total_allowance=Sum("blockallowance__allowance"),
                first_validator_ss58=first_block_validator,
                first_miner_ss58=first_block_miner,
            )
        )

    def validator(self, obj):
        return obj.first_validator_ss58 or "(!) NO BOOKED BLOCKS"

    def miner(self, obj):
        return obj.first_miner_ss58 or "(!) NO BOOKED BLOCKS"

    def block_count(self, obj):
        return obj.block_count or 0

    def total_allowance(self, obj):
        return obj.total_allowance or 0


admin.site.register(Miner, admin_class=MinerReadOnlyAdmin)
admin.site.register(OrganicJob, admin_class=JobReadOnlyAdmin)
admin.site.register(MinerBlacklist, admin_class=MinerBlacklistAdmin)
admin.site.register(AdminJobRequest, admin_class=AdminJobRequestAddOnlyAdmin)
admin.site.register(SystemEvent, admin_class=SystemEventAdmin)
admin.site.register(Weights, admin_class=WeightsReadOnlyAdmin)
admin.site.register(ValidatorWhitelist, admin_class=ValidatorWhitelistAdmin)
admin.site.register(AllowanceMinerManifest, admin_class=AllowanceMinerManifestAdmin)
admin.site.register(BlockAllowance, admin_class=BlockAllowanceAdmin)
admin.site.register(AllowanceBooking, admin_class=AllowanceBookingAdmin)
