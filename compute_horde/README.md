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
  * Make sure to set the expires= option for the beat entry to about the same value to avoid the tasks piling up
* To get the current block number from cache, use `aget_current_block` (or `get_current_block`).

## Validators

This app contains common logic for updating validators of a given network.
To use it, update your `INSTALLED_APPS`:
```python
INSTALLED_APPS = [
    ...,
    'compute_horde.validators',
    ...,
]
```

### Models

This app works with models from other projects; it doesn't define its own.
To use it, you need to specify `COMPUTE_HORDE_VALIDATOR_MODEL` in your `settings.py`,
in the form of `<app_label>.<model_name>`. This model must have at least two fields: public key (string) and active (bool).
Other settings are:

```
COMPUTE_HORDE_VALIDATOR_KEY_FIELD
COMPUTE_HORDE_VALIDATOR_ACTIVE_FIELD
COMPUTE_HORDE_VALIDATOR_DEBUG_FIELD # optional
BITTENSOR_NETUID
BITTENSOR_NETWORK
```

### Tasks

There is a periodic task to update the validators in the database. To use it, define the settings above and add
`compute_horde.validators.tasks.fetch_validators` to your celery beat config.

