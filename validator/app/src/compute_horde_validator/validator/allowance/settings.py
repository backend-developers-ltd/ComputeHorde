RESERVATION_MARGIN_SECONDS = 100

BLOCK_LOOKBACK = 100
# Live block scan will make sure all blocks newer than `now - BLOCK_LOOKBACK` are scraped
ARCHIVE_START_OFFSET = 101
# Archive backfill starts at blocks older than ARCHIVE_START_OFFSET
ARCHIVE_MAX_LOOKBACK = int(361 * 4)
# Archive backfill goes back to ARCHIVE_MAX_LOOKBACK
BLOCK_EVICTION_THRESHOLD = int(ARCHIVE_MAX_LOOKBACK * 1.5)
# blocks older or equal to `now - BLOCK_EVICTION_THRESHOLD` can be evicted from the db
ARCHIVE_SCAN_MAX_RUN_TIME = 300
# Maximum runtime in seconds for each archive scan task
ARCHIVE_SCAN_BATCH_SIZE = 100
# Number of blocks to process before reporting a checkpoint

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
