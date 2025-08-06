import asyncio
import logging
import time

from compute_horde.protocol_messages import (
    GenericError,
    MinerToValidatorMessage,
    V0MainHotkeyMessage,
    ValidatorAuthForMiner,
)
from compute_horde.transport import WSTransport
from compute_horde.utils import sign_blob
from django.conf import settings
from pydantic import TypeAdapter

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
    Async helper to query a single miner.

    Args:
        miner: Miner to query

    Returns:
        Main hotkey (or None)
    """
    transport = None
    try:
        transport = WSTransport(
            f"main_hotkey_query_{miner.hotkey}",
            f"ws://{miner.address}:{miner.port}/v0.1/validator_interface/{settings.BITTENSOR_WALLET().get_hotkey().ss58_address}",
            max_retries=2,
        )
        await transport.start()

        auth_msg = _create_auth_message(miner.hotkey)
        await transport.send(auth_msg.model_dump_json())

        request = V0MainHotkeyMessage()
        await transport.send(request.model_dump_json())

        async with asyncio.timeout(10):
            while True:
                response = await transport.receive()
                if response:
                    msg: MinerToValidatorMessage = TypeAdapter(
                        MinerToValidatorMessage
                    ).validate_json(response)

                    if isinstance(msg, V0MainHotkeyMessage):
                        return msg.main_hotkey
                    elif isinstance(msg, GenericError):
                        logger.warning(f"Error from {miner.hotkey}: {msg.details}")
                        return None

    except TimeoutError:
        logger.warning(f"Timeout querying main hotkey from {miner.hotkey}")
        return None
    except Exception as e:
        logger.warning(f"Error querying main hotkey from {miner.hotkey}: {e}")
        return None
    finally:
        if transport:
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
