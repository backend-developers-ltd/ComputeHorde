import asyncio
import logging

import aiohttp

from compute_horde_validator.validator.models import Miner

logger = logging.getLogger(__name__)


def query_miner_main_hotkeys(miners: list[Miner]) -> dict[str, str | None]:
    """
    Query miners for their main hotkeys.

    Args:
        miners: List of miners to query

    Returns:
        Dictionary mapping miner hotkey to main hotkey (or None)
    """
    return asyncio.run(_query_miners(miners))


async def _query_miners(miners: list[Miner]) -> dict[str, str | None]:
    """
    Query all miners.

    Args:
        miners: List of miners to query

    Returns:
        Dictionary mapping miner hotkey to main hotkey (or None)
    """
    tasks = []
    for miner in miners:
        task = asyncio.create_task(
            _query_single_miner(miner), name=f"query_main_hotkey_{miner.hotkey}"
        )
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    main_hotkeys: dict[str, str | None] = {}
    for i, result in enumerate(results):
        miner = miners[i]
        if isinstance(result, BaseException):
            logger.warning(f"Failed to query main hotkey from {miner.hotkey}: {result}")
            main_hotkeys[miner.hotkey] = None
        else:
            main_hotkeys[miner.hotkey] = result

    return main_hotkeys


async def _query_single_miner(miner: Miner) -> str | None:
    """
    Async helper to query a single miner via HTTP.

    Args:
        miner: Miner to query

    Returns:
        Main hotkey (or None)
    """
    try:
        async with aiohttp.ClientSession() as session:
            url = f"http://{miner.address}:{miner.port}/v0.1/hotkey"
            async with asyncio.timeout(10):
                async with session.get(url) as response:
                    data: dict[str, str | None] = await response.json()
                    if response.status == 200:
                        return data.get("main_hotkey")
                    else:
                        logger.warning(
                            f"HTTP {response.status} from {miner.hotkey}: {data['error']}"
                        )
                        return None

    except TimeoutError:
        logger.warning(f"Timeout querying main hotkey from {miner.hotkey}")
        return None
    except Exception as e:
        logger.warning(f"Error querying main hotkey from {miner.hotkey}: {e}")
        return None
