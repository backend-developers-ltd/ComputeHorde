RESERVATION_MARGIN_SECONDS = 100


# BLOCK_LOOKBACK = 361 * 4
# tasks will make sure all blocks newer than `now - BLOCK_LOOKBACK` are scraped

# BLOCK_EVICTION_THRESHOLD = int(BLOCK_LOOKBACK * 1.5)
# blocks older or equal to `now - BLOCK_EVICTION_THRESHOLD` can be evicted from the db

# TODO: Lookback lowered to 100 due to rate limiting issues on archive subtensor - fix that and increase it back
BLOCK_LOOKBACK = 100
BLOCK_EVICTION_THRESHOLD = int(361 * 4 * 1.5)

BLOCK_EXPIRY = 722
# A job started at block N can be paid for block N-BLOCK_EXPIRY and newer ones

MANIFEST_FETCHING_TIMEOUT = 30.0

MAX_JOB_RUN_TIME = 60 * 60.0
MAX_JOB_RUN_TIME_BLOCKS_APPROX = int(1.1 * MAX_JOB_RUN_TIME // 12)

SPENDING_VALIDATION_BLOCK_LEEWAY_LOWER = 15
# Additional extension of the allowed block range when validating spendings (lower bound)
# TODO: +5: block finalization offset makes the allowance reserve blocks older by 5
# TODO: figure out why we are off by more than that

SPENDING_VALIDATION_BLOCK_LEEWAY_UPPER = 0
# Additional extension of the allowed block range when validating spendings (upper bound)
