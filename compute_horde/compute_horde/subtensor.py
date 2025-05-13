REFERENCE_BLOCK_IN_PEAK_CYCLE = 1000
CYCLES_IN_TESTING_DAY = 10  # 1 peak cycle + 9 non-peak cycles
TEMPO = 360


def get_epoch_containing_block(block: int, netuid: int, tempo: int = TEMPO) -> range:
    """
    Reimplementing the logic from subtensor's Rust function:
        pub fn blocks_until_next_epoch(netuid: u16, tempo: u16, block_number: u64) -> u64
    See https://github.com/opentensor/subtensor.

    See also: https://github.com/opentensor/bittensor/pull/2168/commits/9e8745447394669c03d9445373920f251630b6b8

    The beginning of an epoch is the first block when values like "dividends" are different
    (before an epoch they are constant for a full tempo).
    """
    assert tempo > 0

    interval = tempo + 1
    next_epoch = block + tempo - (block + netuid + 1) % interval

    if next_epoch == block:
        prev_epoch = next_epoch
        next_epoch = prev_epoch + interval
    else:
        prev_epoch = next_epoch - interval

    return range(prev_epoch, next_epoch)


def get_cycle_containing_block(block: int, netuid: int, tempo: int = TEMPO) -> range:
    """
    A cycle contains two epochs, starts on an even one. A cycle is the basic unit of passage of time in compute horde,
    and validators testing miners are synchronised to cycles.
    """
    very_first_epoch = get_epoch_containing_block(0, netuid, tempo=tempo)
    epoch_containing_block = get_epoch_containing_block(block, netuid, tempo=tempo)

    if ((epoch_containing_block.start - very_first_epoch.start) / (tempo + 1)) % 2:
        # that's the second epoch in this cycle
        first_epoch = range(
            epoch_containing_block.start - (tempo + 1), epoch_containing_block.stop - (tempo + 1)
        )
        second_epoch = epoch_containing_block
    else:
        first_epoch = epoch_containing_block
        second_epoch = range(
            epoch_containing_block.start + (tempo + 1), epoch_containing_block.stop + (tempo + 1)
        )

    return range(first_epoch.start, second_epoch.stop)


def get_peak_cycle(block: int, netuid: int, tempo: int = TEMPO) -> range:
    """Get the peak cycle of testing day containing block"""
    cycle_interval = 2 * (tempo + 1)
    testing_day_interval = cycle_interval * CYCLES_IN_TESTING_DAY
    reference_peak_cycle = get_cycle_containing_block(REFERENCE_BLOCK_IN_PEAK_CYCLE, netuid, tempo)
    excess_blocks = (block - reference_peak_cycle.start) % testing_day_interval
    peak_cycle = get_cycle_containing_block(block - excess_blocks, netuid, tempo)
    return peak_cycle
