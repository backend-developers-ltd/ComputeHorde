from django.urls import include, path

from .api import router
from .views import (
    auth_login_view,
    auth_nonce_view,
)

urlpatterns = [
    path("api/v1/", include(router.urls)),
    path("auth/nonce", auth_nonce_view, name="auth_nonce"),
    path("auth/login", auth_login_view, name="auth_login"),
]
