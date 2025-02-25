from unittest.mock import patch

import pytest
from freezegun import freeze_time


@pytest.fixture
def async_sleep_mock():
    with freeze_time() as frozen_time, patch("asyncio.sleep") as sleep_mock:

        async def tick(seconds):
            frozen_time.tick(delta=seconds)

        sleep_mock.side_effect = tick
        yield sleep_mock
