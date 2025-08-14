# Receipts Module

This module provides an interface for managing receipts in the validator.

## Public Interface

### Receipts

The main service class `Receipts` implements the `ReceiptsBase` interface and provides these key methods:

#### Core Methods

- **`scrape_receipts_from_miners(miner_hotkeys, start_block, end_block)`** - Fetch receipts from miners for a block range
- **`create_job_finished_receipt(job_uuid, miner_hotkey, validator_hotkey, time_started, time_took_us, score_str)`** - Create a new job finished receipt
- **`create_job_started_receipt(job_uuid, miner_hotkey, validator_hotkey, executor_class, is_organic, ttl)`** - Create a new job started receipt
- **`get_job_started_receipt_by_uuid(job_uuid)`** - Retrieve a specific job started receipt
- **`get_valid_job_started_receipts_for_miner(miner_hotkey, at_time)`** - Get valid receipts for a miner at a specific time
- **`get_job_finished_receipts_for_miner(miner_hotkey, job_uuids)`** - Get finished receipts for specific jobs from a miner
- **`get_completed_job_receipts_for_block_range(start_block, end_block)`** - Get all completed job receipts within a block range

## Usage Examples

### Basic Receipt Creation

```python
from compute_horde_validator.validator.receipts import Receipts

receipts = Receipts()

# Create a job started receipt
payload, signature = await receipts.create_job_started_receipt(
    job_uuid="job-123",
    miner_hotkey="miner-key",
    validator_hotkey="validator-key",
    executor_class="spin_up-4min.gpu-24gb",
    is_organic=True,
    ttl=300
)

# Create a job finished receipt
finished_receipt = receipts.create_job_finished_receipt(
    job_uuid="job-123",
    miner_hotkey="miner-key",
    validator_hotkey="validator-key",
    time_started=datetime.now(),
    time_took_us=5000000,
    score_str="0.85"
)
```

### Receipt Retrieval

```python
# Get a specific job started receipt
receipt = await receipts.get_job_started_receipt_by_uuid("job-123")
if receipt:
    print(f"Job started at: {receipt.timestamp}")
    print(f"Miner: {receipt.miner_hotkey}")

# Get valid receipts for a miner
valid_receipts = await receipts.get_valid_job_started_receipts_for_miner(
    miner_hotkey="miner-key",
    at_time=datetime.now()
)

# Get finished receipts for specific jobs
finished_receipts = await receipts.get_job_finished_receipts_for_miner(
    miner_hotkey="miner-key",
    job_uuids=["job-123", "job-456"]
)
```

### Receipt Scraping

```python
# Scrape receipts from miners for a block range
scraped_receipts = await receipts.scrape_receipts_from_miners(
    miner_hotkeys=["miner1", "miner2"],
    start_block=1000,
    end_block=2000
)
```

## Background Tasks

### Receipt Scraping Task

The module includes a Celery task for periodic receipt scraping:

**Task Name**: `scrape_receipts_from_miners`

**Purpose**: Automatically fetch and process receipts from miners across the network

**Manual Execution**:
```bash
# Run the task manually (if needed)
celery -A compute_horde_validator call compute_horde_validator.validator.receipts.tasks.scrape_receipts_from_miners
```
