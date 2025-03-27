from django.urls import include, path

from .api import router

urlpatterns = [
    path("api/v1/", include(router.urls)),
]
