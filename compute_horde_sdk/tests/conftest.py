from unittest.mock import patch

import pytest
from freezegun import freeze_time

from tests.utils import INITIAL_FROZEN_TIME


@pytest.fixture
def async_sleep_mock():
    with freeze_time(INITIAL_FROZEN_TIME) as frozen_time, patch("asyncio.sleep") as sleep_mock:

        async def tick(seconds):
            frozen_time.tick(delta=seconds)

        sleep_mock.side_effect = tick
        yield sleep_mock
