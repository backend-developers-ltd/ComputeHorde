from unittest.mock import create_autospec

import pytest
from pylon_client._internal.api.abstract_sync import AbstractIdentityApi, AbstractOpenAccessApi
from pylon_client.v1 import PylonClient


@pytest.fixture
def mock_pylon_client():
    mocked = create_autospec(PylonClient)
    mocked.__enter__.return_value = mocked
    mocked.open_access = create_autospec(AbstractOpenAccessApi, instance=True)
    mocked.identity = create_autospec(AbstractIdentityApi, instance=True)
    return mocked
