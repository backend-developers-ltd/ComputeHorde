import django_filters
from compute_horde_core.output_upload import SingleFileUpload
from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from django.db.models import QuerySet
from django_filters import fields
from django_filters.rest_framework import DjangoFilterBackend
from django_pydantic_field.rest_framework import SchemaField
from rest_framework import mixins, routers, serializers, status, viewsets
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import APIException, ValidationError
from rest_framework.generics import get_object_or_404
from rest_framework.pagination import PageNumberPagination
from rest_framework.permissions import BasePermission, IsAuthenticated
from rest_framework.response import Response
from structlog import get_logger

from .authentication import JWTAuthentication
from .middleware.signature_middleware import require_signature
from .models import CheatedJobReport, Job, JobCreationDisabledError, JobFeedback, JobStatus
from .schemas import MuliVolumeAllowedVolume

logger = get_logger(__name__)


class SmartSchemaField(SchemaField):
    def get_initial(self, *args, **kwargs):
        value = super().get_initial(*args, **kwargs)
        if value is None and getattr(self.schema, "__origin__", None) is list:
            return []
        return value


class Conflict(APIException):
    status_code = status.HTTP_409_CONFLICT
    default_detail = "A conflict occurred."
    default_code = "conflict"


class DefaultModelPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = "page_size"
    max_page_size = 256


class JobStatusEntrySerializer(serializers.ModelSerializer):
    class Meta:
        model = JobStatus
        fields = ["status", "metadata", "created_at"]
        read_only_fields = fields


class JobSerializer(serializers.HyperlinkedModelSerializer):
    """
    - status/stdout/stderr/artifacts/uploads are used by pre-error-propagation SDK
    - newer SDKs should use status_history instead
    """

    class Meta:
        model = Job
        fields = (
            "uuid",
            "executor_class",
            "created_at",
            "last_update",
            "status",
            "docker_image",
            "job_namespace",
            "args",
            "env",
            "use_gpu",
            "tag",
            "stdout",
            "stderr",
            "volumes",
            "uploads",
            "artifacts",
            "artifacts_dir",
            "target_validator_hotkey",
            "on_trusted_miner",
            "upload_results",
            "download_time_limit",
            "execution_time_limit",
            "streaming_start_time_limit",
            "upload_time_limit",
            "streaming_client_cert",
            "streaming_server_cert",
            "streaming_server_address",
            "streaming_server_port",
            "status_history",
        )
        read_only_fields = ("created_at",)

    def to_internal_value(self, data: dict):
        obj = super().to_internal_value(data)
        try:
            obj["streaming_client_cert"] = data["streaming_details"]["public_key"]
            assert isinstance(obj["streaming_client_cert"], str)
        except (KeyError, TypeError, AssertionError):
            obj["streaming_client_cert"] = ""
        return obj

    uploads = SmartSchemaField(schema=list[SingleFileUpload], required=False)
    volumes = SmartSchemaField(schema=list[MuliVolumeAllowedVolume], required=False)

    status = serializers.SerializerMethodField()
    last_update = serializers.SerializerMethodField()
    stdout = serializers.SerializerMethodField()
    stderr = serializers.SerializerMethodField()
    status_history = JobStatusEntrySerializer(many=True, read_only=True, source="statuses")

    def get_status(self, obj):
        # Legacy status field has the "uppercase" enum values
        # for older SDK.
        # TODO: Remove this when nobody is using it any more.
        return obj.status.get_legacy_status_display()

    def get_stdout(self, obj):
        meta = obj.status.meta
        if meta and meta.miner_response:
            return meta.miner_response.docker_process_stdout
        return ""

    def get_stderr(self, obj):
        meta = obj.status.meta
        if meta and meta.miner_response:
            return meta.miner_response.docker_process_stderr
        return ""

    def get_last_update(self, obj):
        return obj.status.created_at


class DockerJobSerializer(JobSerializer):
    class Meta:
        model = Job
        fields = JobSerializer.Meta.fields
        read_only_fields = tuple(
            set(JobSerializer.Meta.fields)
            - {
                "docker_image",
                "job_namespace",
                "executor_class",
                "args",
                "env",
                "use_gpu",
                "tag",
                "volumes",
                "uploads",
                "target_validator_hotkey",
                "artifacts_dir",
                "on_trusted_miner",
                "download_time_limit",
                "execution_time_limit",
                "streaming_start_time_limit",
                "upload_time_limit",
            }
        )


class CheatedJobReportSerializer(serializers.Serializer):
    job_uuid = serializers.UUIDField()
    trusted_job_uuid = serializers.UUIDField()
    details = serializers.JSONField(required=False, allow_null=True)


class JobFeedbackSerializer(serializers.ModelSerializer):
    result_correctness = serializers.FloatField(min_value=0, max_value=1)
    expected_duration = serializers.FloatField(min_value=0, required=False)

    class Meta:
        model = JobFeedback
        fields = ["result_correctness", "expected_duration"]


class RequestHasHotkey(BasePermission):
    def has_permission(self, request, view) -> bool:
        return hasattr(request, "hotkey")


class BaseCreateJobViewSet(mixins.CreateModelMixin, viewsets.GenericViewSet):
    queryset = Job.objects.with_statuses()
    permission_classes = (IsAuthenticated | RequestHasHotkey,)

    def get_authenticators(self) -> list[BaseAuthentication]:
        return super().get_authenticators() + [JWTAuthentication()]

    def perform_create(self, serializer):
        try:
            fields = {"hotkey": self.request.hotkey}
        except AttributeError:
            fields = {"user": self.request.user}
        try:
            serializer.save(**fields, signature=self.request.signature)
        except JobCreationDisabledError as exc:
            raise ValidationError("Job creation is disabled at this moment") from exc
        except ObjectDoesNotExist as exc:
            model_name = exc.__class__.__qualname__.partition(".")[0]
            raise ValidationError(f"Could not select {model_name}")


class NonValidatingMultipleChoiceField(fields.MultipleChoiceField):
    def validate(self, value):
        pass


class NonValidatingMultipleChoiceFilter(django_filters.MultipleChoiceFilter):
    field_class = NonValidatingMultipleChoiceField


class JobViewSetFilter(django_filters.FilterSet):
    uuid = NonValidatingMultipleChoiceFilter(field_name="uuid")

    class Meta:
        model = Job
        fields = ["tag", "uuid"]


class JobViewSet(mixins.RetrieveModelMixin, mixins.ListModelMixin, viewsets.GenericViewSet):
    queryset = Job.objects.with_statuses()
    serializer_class = JobSerializer
    permission_classes = (IsAuthenticated | RequestHasHotkey,)
    pagination_class = DefaultModelPagination
    filter_backends = (DjangoFilterBackend,)
    filterset_class = JobViewSetFilter

    def get_authenticators(self) -> list[BaseAuthentication]:
        authenticators = super().get_authenticators()
        if self.detail:
            authenticators.append(JWTAuthentication())
        return authenticators

    def get_queryset(self) -> QuerySet:
        if hasattr(self.request, "hotkey"):  # noqa: SIM108
            params = {"hotkey": self.request.hotkey}
        else:
            params = {"user": self.request.user}
        return self.queryset.filter(**params)


class DockerJobViewset(BaseCreateJobViewSet):
    serializer_class = DockerJobSerializer


# should fetch job and mark it as cheated
class CheatedJobViewSet(mixins.CreateModelMixin, viewsets.GenericViewSet):
    serializer_class = CheatedJobReportSerializer
    permission_classes = (IsAuthenticated | RequestHasHotkey,)

    def get_authenticators(self) -> list[BaseAuthentication]:
        return super().get_authenticators() + [JWTAuthentication()]

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        validated = serializer.validated_data
        job_uuid = validated["job_uuid"]
        trusted_job_uuid = validated["trusted_job_uuid"]
        details = validated.get("details")

        if hasattr(request, "hotkey"):
            owner_filter = {"hotkey": request.hotkey}
        else:
            owner_filter = {"user": request.user}

        with transaction.atomic():
            try:
                job = Job.objects.select_for_update().get(uuid=job_uuid, on_trusted_miner=False, **owner_filter)
            except Job.DoesNotExist:
                return Response(
                    status=status.HTTP_404_NOT_FOUND,
                    data={"error": "Job not found"},
                )

            try:
                trusted_job = Job.objects.select_for_update().get(
                    uuid=trusted_job_uuid, on_trusted_miner=True, **owner_filter
                )
            except Job.DoesNotExist:
                return Response(
                    status=status.HTTP_404_NOT_FOUND,
                    data={"error": "Trusted job not found"},
                )

            report, created = CheatedJobReport.objects.get_or_create(
                job=job,
                defaults={"trusted_job": trusted_job, "details": details},
            )
            if not created:
                return Response(
                    status=status.HTTP_400_BAD_REQUEST,
                    data={"error": "Cheat report already exists for this job"},
                )

            if not job.cheated:
                job.cheated = True
                job.save(update_fields=["cheated"])

        job.report_cheated(
            signature=request.signature,
            trusted_job_uuid=str(trusted_job_uuid),
            details=details,
        )
        return Response(status=status.HTTP_201_CREATED, data={"message": "Job reported as cheated"})


class JobFeedbackViewSet(mixins.CreateModelMixin, mixins.RetrieveModelMixin, viewsets.GenericViewSet):
    serializer_class = JobFeedbackSerializer
    permission_classes = (IsAuthenticated,)

    def get_queryset(self):
        job_uuid = self.kwargs["job_uuid"]
        return JobFeedback.objects.filter(
            job__uuid=job_uuid,
            job__user=self.request.user,
            user=self.request.user,
        )

    def get_object(self):
        return self.get_queryset().get()

    def get_parent_job(self):
        job_uuid = self.kwargs.get("job_uuid")
        return get_object_or_404(Job, uuid=job_uuid, user=self.request.user)

    def perform_create(self, serializer):
        require_signature(self.request)
        job = self.get_parent_job()
        if JobFeedback.objects.filter(job=job, user=self.request.user).exists():
            raise Conflict("Feedback already exists")

        serializer.save(
            job=job,
            user=self.request.user,
            signature=self.request.signature,
        )

    def get(self, request, *args, **kwargs):
        return self.retrieve(request, *args, **kwargs)

    def put(self, request, *args, **kwargs):
        return self.create(request, *args, **kwargs)


router = routers.SimpleRouter()
router.register(r"jobs", JobViewSet)
router.register(r"job-docker", DockerJobViewset, basename="job_docker")
router.register(r"jobs/(?P<job_uuid>[^/.]+)/feedback", JobFeedbackViewSet, basename="job_feedback")
router.register(r"cheated-job", CheatedJobViewSet, basename="cheated_job")
