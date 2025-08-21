### Receipts module: public interface

This module manages receipt creation and transfer between validators and miners.

## CLI entry point

- **Command**: `python manage.py transfer_receipts`
- **Args**:
  - `--daemon` (flag): run continuously; otherwise runs a single transfer cycle
  - `--debug-miner-hotkey <str>`: fetch only from this miner (debug)
  - `--debug-miner-ip <str>`: debug miner IP
  - `--debug-miner-port <int>`: debug miner port

When all three debug miner parameters are provided, transfer runs in explicit mode for that miner. If not provided and `DEBUG_FETCH_RECEIPTS_FROM_MINERS` is set in settings, transfer runs against those debug miners. Otherwise, miners are resolved from the latest metagraph snapshot.

## Python API

Default implementation lives in `compute_horde_validator.validator.receipts.default.Receipts` and implements the abstract interface in `compute_horde_validator.validator.receipts.base.ReceiptsBase`.

- Run transfer loop (or once):

```python
await receipts().run_receipts_transfer(
    daemon: bool,
    debug_miner_hotkey: str | None,
    debug_miner_ip: str | None,
    debug_miner_port: int | None,
)
```

- Create receipts:

```python
payload, validator_signature = receipts().create_job_started_receipt(
    job_uuid: str,
    miner_hotkey: str,
    validator_hotkey: str,
    executor_class: str,
    is_organic: bool,
    ttl: int,
)

finished = receipts().create_job_finished_receipt(
    job_uuid: str,
    miner_hotkey: str,
    validator_hotkey: str,
    time_started: datetime.datetime,
    time_took_us: int,
    score_str: str,
    block_numbers: list[int] | None = None,  # Optional: list of block numbers used to pay for the job
)
```

- Query receipts:

```python
# All valid JobStarted for a miner at a timestamp
receipts: list[JobStartedReceipt] = await receipts().get_valid_job_started_receipts_for_miner(
    miner_hotkey: str,
    at_time: datetime.datetime,
)

# JobFinished for a miner and a set of job UUIDs
receipts: list[JobFinishedReceipt] = await receipts().get_job_finished_receipts_for_miner(
    miner_hotkey: str,
    job_uuids: list[str],
)
# Raises: No specific exceptions, returns empty list on errors

# JobStarted by job UUID
receipt: JobStartedReceipt = await receipts().get_job_started_receipt_by_uuid(job_uuid: str)
# Raises: JobStartedReceipt.DoesNotExist if receipt not found

# Completed job receipts for a block range [start_block, end_block)
receipts: list[Receipt] = await receipts().get_completed_job_receipts_for_block_range(
    start_block: int,
    end_block: int,
)
```

## Miner selection modes

- **explicit**: when all `debug_miner_*` are passed to `run_receipts_transfer`
- **debug_settings**: when `settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS` is non-empty
- **metagraph**: default; miners are taken from `MetagraphSnapshot`

## Configuration

- **Dynamic config** (fetched via `aget_config`):
  - `DYNAMIC_RECEIPT_TRANSFER_ENABLED: bool` — enable/disable transfer (default: `False`)
  - `DYNAMIC_RECEIPT_TRANSFER_INTERVAL: int` — seconds between polling loops (default: `2`)

- **Settings / env**:
  - `DEBUG_FETCH_RECEIPTS_FROM_MINERS` — list of `"hotkey:ip:port"` values; in settings exposed as
    `settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS: list[tuple[str, str, int]]`
  - `RECEIPT_TRANSFER_CHECKPOINT_CACHE` — cache key namespace used for checkpoints (default: `"receipts_checkpoints"`)

## Metrics (Prometheus)

- `receipttransfer_receipts_total` — number of transferred receipts
- `receipttransfer_miners` — number of miners in the current loop
- `receipttransfer_successful_transfers_total` — count of non-failed transfers
- `receipttransfer_line_errors_total{exc_type}` — per-exception count of line errors
- `receipttransfer_transfer_errors_total{exc_type}` — per-exception count of transfer errors
- `receipttransfer_transfer_duration` — histogram of total loop duration
- `receipttransfer_catchup_pages_left` — gauge of pages left to catch up


