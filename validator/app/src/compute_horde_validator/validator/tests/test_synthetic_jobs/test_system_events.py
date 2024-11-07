from datetime import datetime, timedelta

import pytest
from freezegun import freeze_time

from compute_horde_validator.validator.synthetic_jobs.batch_run import (
    _init_context,
    _not_enough_prompts_system_event,
)


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_not_enough_prompts_system_event_is_not_repeated():
    initial_date = datetime(2020, 1, 1)
    ctx = _init_context(axons={}, serving_miners=[])

    with freeze_time(initial_date) as frozen_datetime:
        await _not_enough_prompts_system_event(ctx, 0)
        assert len(ctx.events) == 1

        frozen_datetime.move_to(initial_date + timedelta(hours=1))

        # still in 24h cached window, should not add event
        await _not_enough_prompts_system_event(ctx, 0)
        assert len(ctx.events) == 1

        frozen_datetime.move_to(initial_date + timedelta(hours=25))

        # after 24h cached window, should add event
        await _not_enough_prompts_system_event(ctx, 0)
        assert len(ctx.events) == 2
