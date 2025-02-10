from datetime import datetime, timedelta

import pytest
from freezegun import freeze_time

from compute_horde_validator.validator.models import Cycle, SyntheticJobBatch, SystemEvent
from compute_horde_validator.validator.synthetic_jobs.batch_run import (
    _init_context,
    _not_enough_prompts_system_event,
)


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_not_enough_prompts_system_event_is_not_repeated():
    initial_date = datetime(2020, 1, 1)
    batch = await SyntheticJobBatch.objects.acreate(
        block=1000,
        cycle=await Cycle.objects.acreate(start=708, stop=1430),
    )
    ctx = await _init_context(axons={}, serving_miners=[], active_validators=[], batch_id=batch.id)

    with freeze_time(initial_date) as frozen_datetime:
        await _not_enough_prompts_system_event(ctx)
        assert len(ctx.events) == 1

        frozen_datetime.move_to(initial_date + timedelta(hours=1))

        # still in 24h cached window, should not add event
        await _not_enough_prompts_system_event(ctx)
        assert len(ctx.events) == 1

        frozen_datetime.move_to(initial_date + timedelta(hours=25))

        # after 24h cached window, should add event
        await _not_enough_prompts_system_event(ctx)
        assert len(ctx.events) == 2


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
# @pytest.mark.override_config(
#     DYNAMIC_SYSTEM_EVENT_LIMITS="MINER_SYNTHETIC_JOB_FAILURE,LLM_PROMPT_ANSWERS_MISSING,2",
# )
async def test_system_event_limits():
    # TODO: Fix constance patching. The override_config marker or the patch_constance decorator
    #       does not patch constance.utils.get_values. So values used from the cache holder
    #       does not get patched.
    #       This test is currently dependent on the default value of the config.

    batch = await SyntheticJobBatch.objects.acreate(
        block=1000,
        cycle=await Cycle.objects.acreate(start=708, stop=1430),
    )
    ctx = await _init_context(axons={}, serving_miners=[], active_validators=[], batch_id=batch.id)

    for _ in range(20):
        # limited type
        ctx.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.LLM_PROMPT_ANSWERS_MISSING,
            description="",
        )
        # not limited type
        ctx.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS,
            subtype=SystemEvent.EventSubType.SUCCESS,
            description="",
        )

    # limited type should be limited
    assert (
        sum(
            1
            for e in ctx.events
            if e.type == SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE
            and e.subtype == SystemEvent.EventSubType.LLM_PROMPT_ANSWERS_MISSING
        )
        == 10
    )
    # not limited type should not be limited
    assert (
        sum(
            1
            for e in ctx.events
            if e.type == SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS
            and e.subtype == SystemEvent.EventSubType.SUCCESS
        )
        == 20
    )
