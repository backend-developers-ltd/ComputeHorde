from django.conf import settings
from django.contrib.admin.sites import site
from django.urls import include, path
from django.views.generic import RedirectView

from .validator.business_metrics import metrics_manager
from .validator.metrics import metrics_view

urlpatterns = [
    path("admin/", site.urls),
    path("", RedirectView.as_view(url="/admin/"), name="home"),
    path("", include("django.contrib.auth.urls")),
    path("metrics", metrics_view, name="prometheus-django-metrics"),
    path("business-metrics", metrics_manager.view, name="prometheus-business-metrics"),
]

if settings.DEBUG_TOOLBAR:
    urlpatterns += [
        path("__debug__/", include("debug_toolbar.urls")),
    ]
