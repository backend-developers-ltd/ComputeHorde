import django_filters
from compute_horde.base.output_upload import SingleFileUpload
from django.core.exceptions import ObjectDoesNotExist
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
from structlog import get_logger

from .authentication import HotkeyAuthentication
from .middleware.signature_middleware import require_signature
from .models import Job, JobCreationDisabledError, JobFeedback
from .schemas import MuliVolumeAllowedVolume
from .utils import safe_config

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


class JobSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Job
        fields = (
            "uuid",
            "executor_class",
            "created_at",
            "last_update",
            "status",
            "docker_image",
            "raw_script",
            "args",
            "env",
            "use_gpu",
            "hf_repo_id",
            "hf_revision",
            "input_url",
            "output_download_url",
            "tag",
            "stdout",
            "volumes",
            "uploads",
            "artifacts",
            "artifacts_dir",
            "target_validator_hotkey",
        )
        read_only_fields = (
            "created_at",
            "output_download_url",
        )

    uploads = SmartSchemaField(schema=list[SingleFileUpload], required=False)
    volumes = SmartSchemaField(schema=list[MuliVolumeAllowedVolume], required=False)

    status = serializers.SerializerMethodField()
    last_update = serializers.SerializerMethodField()
    stdout = serializers.SerializerMethodField()

    def get_status(self, obj):
        return obj.status.get_status_display()

    def get_stdout(self, obj):
        meta = obj.status.meta
        if meta and meta.miner_response:
            return meta.miner_response.docker_process_stdout
        return ""

    def get_last_update(self, obj):
        return obj.status.created_at


class DynamicJobFields:
    def get_fields(self):
        fields = super().get_fields()
        # Check the Constance config value
        if safe_config.JOB_REQUEST_VERSION == 0:
            fields.pop("uploads", None)
            fields.pop("volumes", None)
        return fields


class RawJobSerializer(DynamicJobFields, JobSerializer):
    class Meta:
        model = Job
        fields = JobSerializer.Meta.fields
        read_only_fields = tuple(
            set(JobSerializer.Meta.fields)
            - {"raw_script", "input_url", "hf_repo_id", "hf_revision", "tag", "volumes", "uploads"}
        )


class DockerJobSerializer(DynamicJobFields, JobSerializer):
    class Meta:
        model = Job
        fields = JobSerializer.Meta.fields
        read_only_fields = tuple(
            set(JobSerializer.Meta.fields)
            - {
                "docker_image",
                "executor_class",
                "args",
                "env",
                "use_gpu",
                "input_url",
                "hf_repo_id",
                "hf_revision",
                "tag",
                "volumes",
                "uploads",
                "target_validator_hotkey",
                "artifacts_dir",
            }
        )


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
        return super().get_authenticators() + [HotkeyAuthentication()]

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
            authenticators.append(HotkeyAuthentication())
        return authenticators

    def get_queryset(self) -> QuerySet:
        if hasattr(self.request, "hotkey"):  # noqa: SIM108
            params = {"hotkey": self.request.hotkey}
        else:
            params = {"user": self.request.user}
        return self.queryset.filter(**params)


class RawJobViewset(BaseCreateJobViewSet):
    serializer_class = RawJobSerializer


class DockerJobViewset(BaseCreateJobViewSet):
    serializer_class = DockerJobSerializer


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


class APIRootView(routers.DefaultRouter.APIRootView):
    description = "api-root"


class APIRouter(routers.DefaultRouter):
    APIRootView = APIRootView


router = APIRouter()
router.register(r"jobs", JobViewSet)
router.register(r"job-docker", DockerJobViewset, basename="job_docker")
router.register(r"job-raw", RawJobViewset, basename="job_raw")
router.register(r"jobs/(?P<job_uuid>[^/.]+)/feedback", JobFeedbackViewSet, basename="job_feedback")
