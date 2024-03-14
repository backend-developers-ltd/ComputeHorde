import logging
from typing import TYPE_CHECKING

import bittensor
from more_itertools import one

if TYPE_CHECKING:
    from bittensor.chain_data import NeuronInfo

logger = logging.getLogger(__name__)

BAC_VALIDATOR_SS58_ADDRESS = '5HBVrFGy6oYhhh71m9fFGYD7zbKyAeHnWN8i8s9fJTBMCtEE'
VALIDATORS_LIMIT = 12


def is_neuron_validator(neuron: 'NeuronInfo') -> bool:
    """
    Check if a given neuron is a validator.
    In ComputeHorde subnet, one key cannot be both a miner and a validator.
    """
    is_serving = neuron.axon_info.is_serving
    has_stake = neuron.stake > 0

    if is_serving and has_stake:
        logger.warning("neuron=%s is both serving and has stake", neuron)
    elif not is_serving and not has_stake:
        logger.warning("neuron=%s is neither serving nor has stake", neuron)

    return not is_serving and has_stake


def get_validators(netuid=12, network="finney") -> list['NeuronInfo']:
    """
    Get the keys of top 11 validators of subnet `netuid`.
    This temporarily also includes the validator of BAC for development and testing.
    """
    metagraph = bittensor.metagraph(netuid=netuid, network=network)
    validators = [n for n in metagraph.neurons if is_neuron_validator(n)]

    default_validator = None
    try:
        default_validator = one(
            validator for validator in validators
            if validator.hotkey == BAC_VALIDATOR_SS58_ADDRESS
        )
    except ValueError:
        logger.error('our validator not found')

    validators.sort(key=lambda validator: validator.stake, reverse=True)
    validators = validators[:VALIDATORS_LIMIT]
    if default_validator and default_validator not in validators:
        try:
            validators[VALIDATORS_LIMIT - 1] = default_validator
        except IndexError:
            validators.append(default_validator)

    return validators
