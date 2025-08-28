from django.conf import settings
from django.contrib.admin.sites import site
from django.urls import include, path
from django.views.generic import RedirectView

from .miner.business_metrics import metrics_manager
from .miner.metrics import metrics_view
from .miner.views import get_version, get_main_hotkey, get_manifest

urlpatterns = [
    path("admin/", site.urls),
    path("", RedirectView.as_view(url="/admin/"), name="home"),
    path("", include("django.contrib.auth.urls")),
    path("version", get_version, name="get-version"),
    path("v0.1/hotkey", get_main_hotkey, name="get-main-hotkey"),
    path("v0.1/manifest", get_manifest, name="get-manifest"),
    path("metrics", metrics_view, name="prometheus-django-metrics"),
    path("business-metrics", metrics_manager.view, name="prometheus-business-metrics"),
]

if settings.DEBUG_TOOLBAR:
    urlpatterns += [
        path("__debug__/", include("debug_toolbar.urls")),
    ]
