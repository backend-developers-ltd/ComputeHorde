import asyncio
import logging
import time

from compute_horde.protocol_messages import (
    GenericError,
    MinerToValidatorMessage,
    V0MinerSplitDistributionRequest,
    V0SplitDistributionRequest,
    ValidatorAuthForMiner,
)
from compute_horde.transport import WSTransport
from compute_horde.utils import sign_blob
from django.conf import settings
from pydantic import TypeAdapter

from compute_horde_validator.validator.models import Miner

logger = logging.getLogger(__name__)


def query_miner_split_distributions(miners: list[Miner]) -> dict[str, dict[str, float]]:
    """
    Query miners for their split distributions.

    Args:
        miners: List of miners to query

    Returns:
        Dictionary mapping miner hotkey to split distribution
    """
    return asyncio.run(_query_miners(miners))


async def _query_miners(miners: list[Miner]) -> dict[str, dict[str, float]]:
    """
    Query all miners.

    Args:
        miners: List of miners to query

    Returns:
        Dictionary mapping miner hotkey to split distribution
    """
    tasks = []
    for miner in miners:
        task = asyncio.create_task(
            _query_single_miner(miner), name=f"query_split_{miner.hotkey}"
        )
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    split_distributions: dict[str, dict[str, float]] = {}
    for i, result in enumerate(results):
        miner = miners[i]
        if isinstance(result, BaseException):
            logger.warning(f"Failed to query split distribution from {miner.hotkey}: {result}")
            split_distributions[miner.hotkey] = {}
        else:
            split_distributions[miner.hotkey] = result

    return split_distributions


async def _query_single_miner(miner: Miner) -> dict[str, float]:
    """
    Async helper to query a single miner.

    Args:
        miner: Miner to query

    Returns:
        Split distribution dictionary
    """
    try:
        transport = WSTransport(
        f"split_query_{miner.hotkey}",
        f"ws://{miner.address}:{miner.port}/v0.1/validator_interface/{settings.BITTENSOR_WALLET().get_hotkey().ss58_address}",
        max_retries=2,
    )
        await transport.start()

        auth_msg = _create_auth_message(miner.hotkey)
        await transport.send(auth_msg.model_dump_json())

        request = V0SplitDistributionRequest()
        await transport.send(request.model_dump_json())

        async with asyncio.timeout(10):
            while True:
                response = await transport.receive()
                if response:
                    # Parse response
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
                    else:
                        logger.warning(f"Unexpected message type from {miner.hotkey}: {type(msg)}")
                        await transport.stop()
                        return {}

    except TimeoutError:
        logger.warning(f"Timeout querying split distribution from {miner.hotkey}")
        await transport.stop()
        return {}
    except Exception as e:
        logger.warning(f"Error querying split distribution from {miner.hotkey}: {e}")
        await transport.stop()
        return {}
    finally:
        await transport.stop()


def _create_auth_message(miner_hotkey: str) -> ValidatorAuthForMiner:
    """
    Create authentication message for miner.

    Args:
        miner_hotkey: Miner hotkey

    Returns:
        Authentication message
    """
    wallet = settings.BITTENSOR_WALLET()
    hotkey = wallet.get_hotkey()

    msg = ValidatorAuthForMiner(
        validator_hotkey=hotkey.ss58_address,
        miner_hotkey=miner_hotkey,
        timestamp=int(time.time()),
        signature="",
    )
    msg.signature = sign_blob(hotkey, msg.blob_for_signing())
    return msg
