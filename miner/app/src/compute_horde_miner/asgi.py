import os


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'compute_horde_miner.settings')
import django

django.setup(
)  # that's because channels doesn't support ROOT_URLCONF AFAIK and the ASGI application (URLRouter) has
# to import consumers which import models

from channels.routing import URLRouter
from django.core.asgi import get_asgi_application
from django.urls import path
from django.urls import re_path
from channels.routing import ProtocolTypeRouter

from .miner.miner_consumer.validator_interface import MinerValidatorConsumer
from .miner.miner_consumer.executor_interface import MinerExecutorConsumer

application = ProtocolTypeRouter({
    'http': URLRouter([
        re_path(r'.*', get_asgi_application()),
    ]),
    'websocket': URLRouter([
        path('v0/validator_interface', MinerValidatorConsumer.as_asgi()),
        path('v0/executor_interface', MinerExecutorConsumer.as_asgi()),
    ]),
})

