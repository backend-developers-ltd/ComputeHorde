from django.apps import AppConfig


class ComputeHordeBlockchainConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "compute_horde.blockchain"
    verbose_name = "Blockchain"

    def ready(self):
        from django.conf import settings

        # Check if a required setting is defined
        if not hasattr(settings, "BITTENSOR_NETWORK"):
            raise ValueError(
                "The setting 'BITTENSOR_NETWORK' is required in your settings.py "
                "for 'compute_horde.blockchain' to function properly. Please define it."
            )
