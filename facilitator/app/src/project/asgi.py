import logging
import os
from importlib import import_module

import django
from channels.routing import ProtocolTypeRouter, URLRouter
from django.conf import settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "project.settings")
django.setup()

from .urls import ws_urlpatterns  # NOQA: E402

log = logging.getLogger(__name__)


log.info("Using ASGI application: %s", settings.HTTP_ASGI_APPLICATION_PATH)
module_path, function_name = settings.HTTP_ASGI_APPLICATION_PATH.rsplit(".", maxsplit=1)
module = import_module(module_path)
get_asgi_application = getattr(module, function_name)

application = ProtocolTypeRouter(
    {
        "http": get_asgi_application(),
        "websocket": URLRouter(ws_urlpatterns),
    }
)
