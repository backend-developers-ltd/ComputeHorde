import asyncio
import datetime
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar

import bittensor
import pydantic
from bittensor.core.errors import SubstrateRequestException

BAC_VALIDATOR_SS58_ADDRESS = "5HBVrFGy6oYhhh71m9fFGYD7zbKyAeHnWN8i8s9fJTBMCtEE"
REQUIERED_VALIDATORS = (
    BAC_VALIDATOR_SS58_ADDRESS,
    "5FFApaS75bv5pJHfAp2FVLBj9ZaXuFDjEypsaBNc1wCfe52v",
    "5F4tQyWrhfGVcNhoqeiNsR6KjD4wMZ2kfhLj4oHYuyHbZAc3",
    "5F2CsUDVbRbVMXTh9fAzF9GacjVX7UapvRxidrxe7z8BYckQ",
    "5HEo565WAy4Dbq3Sv271SAi7syBSofyfhhwRNjFNSM2gP9M2",
)
MIN_STAKE = 1000
VALIDATORS_LIMIT = 24


class MachineSpecs(pydantic.BaseModel):
    specs: dict[Any, Any]

    def __str__(self) -> str:
        return str(self.specs)


class ValidatorListError(Exception):
    def __init__(self, reason: Exception):
        self.reason = reason


def get_validators(
    metagraph: bittensor.Metagraph | None = None,
    netuid=12,
    network="finney",
    block: int | None = None,
) -> list[bittensor.NeuronInfo]:
    """
    Validators are top 24 neurons in terms of stake, only taking into account those that have at least 1000
    and forcibly including BAC_VALIDATOR_SS58_ADDRESS.
    The result is sorted.
    """
    if metagraph is None:
        try:
            subtensor = bittensor.subtensor(network=network)
        except Exception as ex:
            raise ValidatorListError(ex) from ex

        try:
            metagraph = subtensor.metagraph(netuid, block=block)
        except SubstrateRequestException as ex:
            raise ValidatorListError(ex) from ex

    neurons = [
        n for n in metagraph.neurons if n.hotkey in REQUIERED_VALIDATORS or n.stake >= MIN_STAKE
    ]
    neurons = sorted(
        neurons, key=lambda n: (n.hotkey == BAC_VALIDATOR_SS58_ADDRESS, n.stake), reverse=True
    )
    return neurons[:VALIDATORS_LIMIT]


def json_dumps_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()

    raise TypeError


class Timer:
    def __init__(self, timeout=None):
        self.start_time = datetime.datetime.now()
        self.timeout = timeout

    def passed_time(self):
        return (datetime.datetime.now() - self.start_time).total_seconds()

    def time_left(self):
        if self.timeout is None:
            raise ValueError("timeout was not specified")
        return self.timeout - self.passed_time()


def sign_blob(kp: bittensor.Keypair, blob: str):
    """
    Signs a string blob with a bittensor keypair and returns the signature
    """
    return f"0x{kp.sign(blob).hex()}"


P = ParamSpec("P")
R = TypeVar("R")


def async_synchronized(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """
    Wraps the function in an async lock.
    """
    lock = asyncio.Lock()

    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        async with lock:
            return await func(*args, **kwargs)

    return wrapper
