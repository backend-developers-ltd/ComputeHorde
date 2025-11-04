from compute_horde.utils import (
    BAC_VALIDATOR_SS58_ADDRESS,
    MIN_VALIDATOR_STAKE,
    VALIDATORS_LIMIT,
    ValidatorInfo,
)

from compute_horde_validator.validator.allowance.types import MetagraphData


# TODO: remove this function when all dependants moved to allowance
def get_validator_infos(
    metagraph: MetagraphData,
) -> list[ValidatorInfo]:
    """
    Validators are top 24 neurons in terms of stake, only taking into account those that have at least 1000
    and forcibly including BAC_VALIDATOR_SS58_ADDRESS.
    The result is sorted.
    """
    validators = [
        (uid, hotkey, stake)
        for uid, hotkey, stake in zip(metagraph.uids, metagraph.hotkeys, metagraph.total_stake)
        if stake >= MIN_VALIDATOR_STAKE
    ]
    top_validators = sorted(
        validators, key=lambda data: (data[1] == BAC_VALIDATOR_SS58_ADDRESS, data[2]), reverse=True
    )[:VALIDATORS_LIMIT]
    return [
        ValidatorInfo(uid=uid, hotkey=hotkey, stake=stake) for uid, hotkey, stake in top_validators
    ]
