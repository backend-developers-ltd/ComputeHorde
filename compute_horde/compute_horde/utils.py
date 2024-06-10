import datetime
from typing import TYPE_CHECKING

import bittensor
import pydantic

if TYPE_CHECKING:
    from bittensor.chain_data import NeuronInfo

BAC_VALIDATOR_SS58_ADDRESS = "5HBVrFGy6oYhhh71m9fFGYD7zbKyAeHnWN8i8s9fJTBMCtEE"
MIN_STAKE = 1000
VALIDATORS_LIMIT = 24


class MachineSpecs(pydantic.BaseModel):
    specs: dict

    def __str__(self) -> str:
        return str(self.specs)


def get_validators(netuid=12, network="finney") -> list["NeuronInfo"]:
    """
    Validators are top 24 neurons in terms of stake, only taking into account those that have at least 1000
    and forcibly including BAC_VALIDATOR_SS58_ADDRESS.
    The result is sorted.
    """
    metagraph = bittensor.metagraph(netuid=netuid, network=network)
    neurons = [
        n
        for n in metagraph.neurons
        if (n.hotkey == BAC_VALIDATOR_SS58_ADDRESS or n.stake.tao >= MIN_STAKE)
    ]
    neurons = sorted(
        neurons, key=lambda n: (n.hotkey == BAC_VALIDATOR_SS58_ADDRESS, n.stake), reverse=True
    )
    return neurons[:VALIDATORS_LIMIT]


def _json_dumps_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()

    raise TypeError
