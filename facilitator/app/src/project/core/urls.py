from django.urls import include, path
from rest_framework.authtoken.views import obtain_auth_token
from rest_framework.schemas import get_schema_view

from .api import router
from .views import (
    DockerImageJobCreateView,
    JobDetailView,
    JobListView,
    RawScriptJobCreateView,
    api_token_view,
    auth_login_view,
    auth_nonce_view,
)

urlpatterns = [
    path("", JobListView.as_view(), name="job/list"),
    path("new-docker/", DockerImageJobCreateView.as_view(), name="job-docker/submit"),
    path("new-raw/", RawScriptJobCreateView.as_view(), name="job-raw/submit"),
    path("<uuid:pk>/", JobDetailView.as_view(), name="job/detail"),
    path("api/v1/", include(router.urls)),
    path("api-auth/", include("rest_framework.urls")),
    path("api-token-auth/", obtain_auth_token, name="api-token-auth"),
    path("api-token-generate/", api_token_view, name="api-token-generate"),
    path(
        "api/v1/openapi",
        get_schema_view(title="Compute Horde Facilitator", description="API creating organic jobs", version="1.0.0"),
        name="openapi-schema",
    ),
    path("auth/nonce", auth_nonce_view, name="auth_nonce"),
    path("auth/login", auth_login_view, name="auth_login"),
]
