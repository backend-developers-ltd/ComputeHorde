import os

from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application
from django.urls import path, re_path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compute_horde_miner.settings")
import django  # noqa

django.setup()  # that's because channels doesn't support ROOT_URLCONF AFAIK and the ASGI application (URLRouter) has
# to import consumers which import models

from .miner.miner_consumer.executor_interface import MinerExecutorConsumer  # noqa
from .miner.miner_consumer.validator_interface import MinerValidatorConsumer  # noqa

application = ProtocolTypeRouter(
    {
        "http": URLRouter(
            [
                re_path(r".*", get_asgi_application()),
            ]
        ),
        "websocket": URLRouter(
            [
                path("v0/validator_interface/<str:validator_key>", MinerValidatorConsumer.as_asgi()),
                path("v0/executor_interface/<str:executor_token>", MinerExecutorConsumer.as_asgi()),
            ]
        ),
    }
)
