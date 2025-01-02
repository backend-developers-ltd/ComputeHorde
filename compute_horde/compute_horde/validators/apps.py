from django.apps import AppConfig
from django.core.exceptions import ImproperlyConfigured

REQUIRED_SETTINGS = [
    "VALIDATOR_MODEL",
    "VALIDATOR_KEY_FIELD",
    "VALIDATOR_ACTIVE_FIELD",
    # VALIDATOR_DEBUG_FIELD - optional
    "BITTENSOR_NETUID",
    "BITTENSOR_NETWORK",
]


class ValidatorsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "compute_horde.validators"
    verbose_name = "validators"

    def ready(self):
        from django.conf import settings

        # Check if required settings are defined
        missing_settings = [
            setting for setting in REQUIRED_SETTINGS if not hasattr(settings, setting)
        ]

        if missing_settings:
            raise ImproperlyConfigured(
                f"The settings: {', '.join(missing_settings)} are required in your settings.py "
                "for 'compute_horde.validators' to function properly. Please define them."
            )
