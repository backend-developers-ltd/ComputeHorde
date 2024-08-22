from compute_horde_validator.validator.dynamic_config import aget_config, aget_weights_version


async def get_manifest_multiplier(
    previous_online_executors: int | None, current_online_executors: int
) -> float | None:
    weights_version = await aget_weights_version()
    multiplier = None
    if weights_version >= 2:
        if previous_online_executors is None:
            multiplier = await aget_config("DYNAMIC_MANIFEST_SCORE_MULTIPLIER")
        else:
            low, high = sorted([previous_online_executors, current_online_executors])
            # low can be 0 if previous_online_executors == 0, but we make it that way to
            # make this function correct for any kind of input
            threshold = await aget_config("DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD")
            if low == 0 or high / low >= threshold:
                multiplier = await aget_config("DYNAMIC_MANIFEST_SCORE_MULTIPLIER")
    return multiplier
