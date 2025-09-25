RESERVATION_MARGIN_SECONDS = 100

BLOCK_LOOKBACK = 361 * 4
# tasks will make sure all blocks newer than `now - BLOCK_LOOKBACK` are scraped

BLOCK_EVICTION_THRESHOLD = int(BLOCK_LOOKBACK * 1.5)
# blocks older or equal to `now - BLOCK_EVICTION_THRESHOLD` can be evicted from the db

BLOCK_EXPIRY = 722
# A job started at block N can be paid for block N-BLOCK_EXPIRY and newer ones

MANIFEST_FETCHING_TIMEOUT = 30.0

MAX_JOB_RUN_TIME = 60 * 60.0
MAX_JOB_RUN_TIME_BLOCKS_APPROX = int(MAX_JOB_RUN_TIME * 1.1 // 12)

SPENDING_VALIDATION_BLOCK_LEEWAY_LOWER = 7
# Additional extension of the allowed block range when validating spendings (lower bound)
# TODO: +5: block finalization offset makes the allowance reserve blocks older by 5
# TODO: +2: we are still off by 2 in what the allowance module reserves, even accounting for finalization

SPENDING_VALIDATION_BLOCK_LEEWAY_UPPER = 0
# Additional extension of the allowed block range when validating spendings (upper bound)
