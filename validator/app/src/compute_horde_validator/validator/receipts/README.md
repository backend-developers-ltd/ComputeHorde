# Receipts Module

This module provides an interface for managing receipts in the validator.

## Basic Usage

```python
from compute_horde_validator.validator.receipts import Receipts

# Create receipts manager
receipts = Receipts()

# Get completed job receipts for scoring
completed_receipts = receipts.get_completed_job_receipts_for_block_range(
    start_block=1000, 
    end_block=2000
)

# Create a job finished receipt
receipt = receipts.create_job_finished_receipt(
    job_uuid="job-123",
    miner_hotkey="miner_hotkey",
    validator_hotkey="validator_hotkey",
    time_started=1640995200,
    time_took_us=5000000,
    score_str="0.85"
)

# Save the receipt
receipts.save_receipt(receipt.to_receipt())

# Scrape receipts from miners
scraped_receipts = await receipts.scrape_receipts_from_miners(["miner1", "miner2"])
```

## Core Functionality

### Receipts Retrieval

The primary method for retrieve methods in given block range:

```python
# Get completed job receipts for scoring
completed_receipts = manager.get_completed_job_receipts_for_block_range(
    start_block=1000, 
    end_block=2000
)
```

### Receipt Creation

The module can create receipts for completed jobs:

```python
# Create job finished receipt
receipt = receipts.create_job_finished_receipt(
    job_uuid="job-123",
    miner_hotkey="miner_hotkey",
    validator_hotkey="validator_hotkey",
    time_started=1640995200,  # Unix timestamp
    time_took_us=5000000,     # 5 seconds in microseconds
    score_str="0.85"
)
```

### Receipt Scraping

The module can scrape receipts from miners:

```python
# Scrape receipts from specific miners
scraped_receipts = await receipts.scrape_receipts_from_miners([
    "miner_hotkey_1",
    "miner_hotkey_2"
], start_block=1000, end_block=2000)
```

### Receipt Persistence

The module provides methods to save and retrieve receipts:

```python
# Save a receipt to the database
receipts.save_receipt(receipt)

# Get a receipt by job UUID
receipt = receipts.get_receipt_by_job_uuid("job-123")
```

## Integration with compute_horde

The receipts module uses the `compute_horde.receipts` module internally for:
- Receipt models and schemas
- Receipt validation and serialization
- Receipt transfer functionality
- Database models and migrations

## Integration with Scoring

The receipts module is designed to work seamlessly with the scoring system:

1. **Block-based filtering**: Provides method to get receipts for specific block ranges
2. **Completed job receipts**: Specialized method for getting receipts of completed jobs
3. **Scoring data extraction**: Receipts contain all necessary data for scoring calculations
4. **Performance metrics**: Job finished receipts include timing and score information

## Error Handling

The module provides specific exceptions for different error scenarios:

```python
from compute_horde_validator.validator.receipts.exceptions import (
    ReceiptsConfigurationError,
    ReceiptsScrapingError,
    ReceiptsGenerationError,
)

try:
    receipt = receipts.create_job_finished_receipt(...)
except ReceiptsGenerationError as e:
    # Handle generation error
    pass
```
