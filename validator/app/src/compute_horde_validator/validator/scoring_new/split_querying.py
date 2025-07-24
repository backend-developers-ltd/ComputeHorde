import asyncio
import logging
import time

from compute_horde.protocol_messages import (
    GenericError,
    V0MinerSplitDistributionRequest,
    V0SplitDistributionRequest,
    ValidatorAuthForMiner,
)
from compute_horde.transport import WSTransport
from compute_horde.transport.base import TransportConnectionError
from compute_horde.utils import sign_blob
from django.conf import settings

from compute_horde_validator.validator.models import Miner

logger = logging.getLogger(__name__)


async def query_miner_split_distributions(miners: list[Miner]) -> dict[str, dict[str, float]]:
    """
    Query miners for their split distributions.

    Args:
        miners: List of miners to query

    Returns:
        Dictionary mapping miner hotkey to split distribution
    """
    split_distributions: dict[str, dict[str, float]] = {}

    # Create tasks for concurrent querying
    tasks = []
    for miner in miners:
        task = asyncio.create_task(
            _query_single_miner_split_distribution(miner), name=f"query_split_{miner.hotkey}"
        )
        tasks.append(task)

    # Wait for all queries to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    for i, result in enumerate(results):
        miner = miners[i]
        if isinstance(result, BaseException):
            logger.warning(f"Failed to query split distribution from {miner.hotkey}: {result}")
            split_distributions[miner.hotkey] = {}
        else:
            split_distributions[miner.hotkey] = result

    return split_distributions


async def _query_single_miner_split_distribution(miner: Miner) -> dict[str, float]:
    """
    Query a single miner for its split distribution.

    Args:
        miner: Miner to query

    Returns:
        Split distribution dictionary
    """
    # Create transport
    transport = WSTransport(
        f"split_query_{miner.hotkey}",
        f"ws://{miner.address}:{miner.port}/v0.1/validator_interface/{settings.BITTENSOR_WALLET().get_hotkey().ss58_address}",
        max_retries=2,
    )

    try:
        # Connect to miner
        await transport.connect()

        # Send authentication
        auth_msg = _create_auth_message(miner.hotkey)
        await transport.send(auth_msg.model_dump_json())

        # Send split distribution request
        request = V0SplitDistributionRequest()
        await transport.send(request.model_dump_json())

        # Wait for response with timeout
        async with asyncio.timeout(10):  # 10 second timeout
            while True:
                response = await transport.receive()
                if response:
                    # Parse response
                    from compute_horde.protocol_messages import MinerToValidatorMessage
                    from pydantic import TypeAdapter

                    msg: MinerToValidatorMessage = TypeAdapter(
                        MinerToValidatorMessage
                    ).validate_json(response)

                    if isinstance(msg, V0MinerSplitDistributionRequest):
                        await transport.stop()
                        return msg.split_distribution
                    elif isinstance(msg, GenericError):
                        logger.warning(f"Error from {miner.hotkey}: {msg.details}")
                        await transport.stop()
                        return {}

    except (TimeoutError, TransportConnectionError, Exception) as e:
        logger.warning(f"Failed to query split distribution from {miner.hotkey}: {e}")
        return {}
    finally:
        try:
            await transport.stop()
        except Exception:
            pass


def _create_auth_message(miner_hotkey: str) -> ValidatorAuthForMiner:
    """Create authentication message for miner."""
    from compute_horde.protocol_messages import ValidatorAuthForMiner

    wallet = settings.BITTENSOR_WALLET()
    keypair = wallet.get_hotkey()

    msg = ValidatorAuthForMiner(
        validator_hotkey=keypair.ss58_address,
        miner_hotkey=miner_hotkey,
        timestamp=int(time.time()),
        signature="",
    )
    msg.signature = sign_blob(keypair, msg.blob_for_signing())
    return msg
