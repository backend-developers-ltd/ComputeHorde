from django.conf import settings
from pylon._internal.client.sync.client import PylonClient
from pylon._internal.client.sync.config import DEFAULT_RETRIES, Config
from tenacity import Retrying


def pylon_client(retries: Retrying = DEFAULT_RETRIES) -> PylonClient:
    return PylonClient(
        Config(
            address=settings.PYLON_ADDRESS,
            identity_name=settings.PYLON_IDENTITY_NAME,
            identity_token=settings.PYLON_IDENTITY_TOKEN,
            retry=retries,
        )
    )
