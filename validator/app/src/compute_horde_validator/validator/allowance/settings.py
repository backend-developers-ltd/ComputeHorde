RESERVATION_MARGIN_SECONDS = 100

BLOCK_LOOKBACK = 361 * 4
# tasks will make sure all blocks newer than `now - BLOCK_LOOKBACK` are scraped

BLOCK_EVICTION_THRESHOLD = int(BLOCK_LOOKBACK * 1.5)
# blocks older or equal to `now - BLOCK_EVICTION_THRESHOLD` can be evicted from the db

BLOCK_EXPIRY = 722
# A job started at block N can be paid for block N-BLOCK_EXPIRY and newer ones

BLOCK_FINALIZATION_OFFSET = 5
# The offset from the current block that we consider to be "finalized"

MANIFEST_FETCHING_TIMEOUT = 30.0

MAX_JOB_RUN_TIME = 60 * 60.0
