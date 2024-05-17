import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compute_horde_executor.settings")

application = get_asgi_application()
