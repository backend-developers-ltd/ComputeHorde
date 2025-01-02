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
To use it, you need to specify `VALIDATOR_MODEL` in your `settings.py`,
in the form of `<app_label>.<model_name>`. This model must have at least two fields: public key (string) and active (bool).
Other settings are:

```
VALIDATOR_KEY_FIELD
VALIDATOR_ACTIVE_FIELD
VALIDATOR_DEBUG_FIELD # optional
BITTENSOR_NETUID
BITTENSOR_NETWORK
```

### Tasks

There is a periodic task to update the validators in the database. To use it, define the settings above and add
`compute_horde.validators.tasks.fetch_validators` to your celery beat config.

