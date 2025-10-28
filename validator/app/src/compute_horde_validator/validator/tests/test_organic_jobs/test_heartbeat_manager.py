import asyncio

import pytest
from channels.layers import get_channel_layer

from compute_horde_validator.validator.organic_jobs.facilitator_client.constants import (
    HEARTBEAT_CHANNEL,
)
from compute_horde_validator.validator.organic_jobs.facilitator_client.heartbeat_manager import (
    HeartbeatManager,
)


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_heartbeat_manager_sends_heartbeats():
    heartbeat_manager = HeartbeatManager()
    heartbeat_manager.HEARTBEAT_INTERVAL = 0.1
    await heartbeat_manager.start()

    # Collect some heartbeats
    layer = get_channel_layer()
    heartbeats = []
    try:
        async with asyncio.timeout(1):
            while True:
                heartbeats.append(await layer.receive(HEARTBEAT_CHANNEL))
    except TimeoutError:
        pass
    finally:
        await heartbeat_manager.stop()

    assert len(heartbeats) > 0
