from django.conf import settings
from pylon_client.v1 import PylonClient
from pylon_client.v1 import DEFAULT_RETRIES, Config
from tenacity import Retrying

"""
PylonClient factory for all subprojects. If you need it mocked in test cases, look at compute_horde/test_base/pylon.py.
"""


def pylon_client_with_identity(retries: Retrying = DEFAULT_RETRIES) -> PylonClient:
    return PylonClient(
        Config(
            address=settings.PYLON_ADDRESS,  # type: ignore
            identity_name=settings.PYLON_IDENTITY_NAME,  # type: ignore
            identity_token=settings.PYLON_IDENTITY_TOKEN,  # type: ignore
            retry=retries,
        )
    )


def pylon_client_with_open_access(retries: Retrying = DEFAULT_RETRIES) -> PylonClient:
    return PylonClient(
        Config(
            address=settings.PYLON_ADDRESS,  # type: ignore
            open_access_token=settings.PYLON_OPEN_ACCESS_TOKEN,  # type: ignore
            retry=retries,
        )
    )
