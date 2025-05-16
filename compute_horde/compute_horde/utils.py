import asyncio
import dataclasses
import datetime
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar

import bittensor
import pydantic
from bittensor.core.errors import SubstrateRequestException

BAC_VALIDATOR_SS58_ADDRESS = "5HBVrFGy6oYhhh71m9fFGYD7zbKyAeHnWN8i8s9fJTBMCtEE"
MIN_STAKE = 1000
VALIDATORS_LIMIT = 24


class MachineSpecs(pydantic.BaseModel):
    specs: dict[Any, Any]

    def __str__(self) -> str:
        return str(self.specs)


class ValidatorListError(Exception):
    def __init__(self, reason: Exception):
        self.reason = reason


@dataclasses.dataclass
class ValidatorInfo:
    uid: int
    hotkey: str
    stake: float  # total effective stake denominated in alpha tokens


def get_validators(
    metagraph: bittensor.Metagraph | None = None,
    netuid=12,
    network="finney",
    block: int | None = None,
) -> list[ValidatorInfo]:
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
        n
        for n in metagraph.neurons
        if (n.hotkey == BAC_VALIDATOR_SS58_ADDRESS or metagraph.total_stake[n.uid] >= MIN_STAKE)
    ]
    neurons = sorted(
        neurons,
        key=lambda n: (n.hotkey == BAC_VALIDATOR_SS58_ADDRESS, metagraph.total_stake[n.uid]),
        reverse=True,
    )
    return [
        ValidatorInfo(uid=n.uid, hotkey=n.hotkey, stake=float(metagraph.total_stake[n.uid]))
        for n in neurons[:VALIDATORS_LIMIT]
    ]


def json_dumps_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()

    raise TypeError


class Timer:
    def __init__(self, timeout: float | None = None) -> None:
        self.start_time = datetime.datetime.now()
        self.timeout = timeout

    def set_timeout(self, seconds: float) -> None:
        self.start_time = datetime.datetime.now()
        self.timeout = seconds

    def extend_timeout(self, seconds: float) -> None:
        if self.timeout is None:
            raise ValueError("timeout was not specified")
        self.timeout += seconds

    def passed_time(self) -> float:
        return (datetime.datetime.now() - self.start_time).total_seconds()

    def time_left(self) -> float:
        if self.timeout is None:
            raise ValueError("timeout was not specified")
        return self.timeout - self.passed_time()


def sign_blob(kp: bittensor.Keypair, blob: str) -> str:
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
