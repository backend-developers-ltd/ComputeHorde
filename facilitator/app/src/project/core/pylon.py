from django.conf import settings
from pylon_client.v1 import DEFAULT_RETRIES, Config, PylonClient
from tenacity import Retrying


def pylon_client(retries: Retrying = DEFAULT_RETRIES) -> PylonClient:
    return PylonClient(
        Config(
            address=settings.PYLON_ADDRESS,
            open_access_token=settings.PYLON_OPEN_ACCESS_TOKEN,
            retry=retries,
        )
    )
