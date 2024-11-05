from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from compute_horde_validator.validator.cross_validation.utils import (
    trusted_miner_not_configured_system_event,
)


@pytest.mark.asyncio
async def test_trusted_miner_not_configured_system_event_is_not_repeated():
    initial_date = datetime(2020, 1, 1)
    with patch("compute_horde_validator.validator.cross_validation.utils.SystemEvent") as mock:
        with freeze_time(initial_date) as frozen_datetime:
            await trusted_miner_not_configured_system_event(...)
            assert mock.objects.create.call_count == 1

            frozen_datetime.move_to(initial_date + timedelta(hours=1))

            # still in 24h cached window, should not call create again
            await trusted_miner_not_configured_system_event(...)
            assert mock.objects.create.call_count == 1

            frozen_datetime.move_to(initial_date + timedelta(hours=25))

            # after 24h cached window, should call create
            await trusted_miner_not_configured_system_event(...)
            assert mock.objects.create.call_count == 2
