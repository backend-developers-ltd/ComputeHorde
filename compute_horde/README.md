# compute-horde

## Receipts

### Common django models

This library contains common Django models for the receipts.
To use them, update your `INSTALLED_APPS`:
```python
INSTALLED_APPS = [
    ...,
    'compute_horde.receipts',
    ...,
]
```

### Migrations

To make new migrations after doing some changes in the model files, run:
```shell
DJANGO_SETTINGS_MODULE=compute_horde.settings uv run django-admin makemigrations
```

## Blockchain

This library contains common logic for interacting with the blockchain (subtensor).

To use it, update your `INSTALLED_APPS`:
```python
INSTALLED_APPS = [
    ...,
    'compute_horde.blockchain',
    ...,
]
```

### Block cache

This is a cheap-to-query store that stores the current block number.

Usage:

* Add `compute_horde.blockchain.tasks.update_block_cache` as a Celery Beat task with some reasonable frequency (like 1 or 2 s)
* To get the current block number from cache, use `aget_current_block` (or `get_current_block`).
