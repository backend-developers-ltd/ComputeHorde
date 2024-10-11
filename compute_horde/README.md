# compute-horde

## Common django models
This library contains common Django models for the receipts.
To use them, update your `INSTALLED_APPS`:
```python
INSTALLED_APPS = [
    ...,
    'compute_horde.receipts',
    ...,
]
```

## Migrations
To make new migrations after doing some changes in the model files, run:
```shell
DJANGO_SETTINGS_MODULE=compute_horde.settings pdm run django-admin makemigrations
```
