import logging

from django.apps import AppConfig
from django.conf import settings
from django.db.models.signals import post_migrate

logger = logging.getLogger(__name__)


def maybe_create_default_admin(sender, **kwargs):
    from django.contrib.auth.models import User  # noqa

    # Create default admin user if missing
    admin_user_exists = User.objects.filter(is_superuser=True).exists()
    if not admin_user_exists:
        admin_password = settings.DEFAULT_ADMIN_PASSWORD
        if admin_password is None:
            logger.warning(
                "Not creating Admin user - please set DEFAULT_ADMIN_PASSWORD env variable"
            )
        else:
            logger.info("Creating Admin user with DEFAULT_ADMIN_PASSWORD")
            User.objects.create_superuser(
                username=settings.DEFAULT_ADMIN_USERNAME,
                email=settings.DEFAULT_ADMIN_EMAIL,
                password=admin_password,
            )


def create_local_miner_validator(sender, **kwargs):
    if not settings.IS_LOCAL_MINER:
        return

    assert (
        settings.LOCAL_MINER_VALIDATOR_PUBLIC_KEY
    ), "LOCAL_MINER_VALIDATOR_PUBLIC_KEY needs to be set to run miner in local mode"

    from .models import Validator

    instance, created = Validator.objects.get_or_create(
        public_key=settings.LOCAL_MINER_VALIDATOR_PUBLIC_KEY,
        defaults={"active": True, "debug": False},
    )
    if created:
        logger.info("Created validator with public key %s", instance.public_key)


class MinerConfig(AppConfig):
    name = "compute_horde_miner.miner"

    def ready(self):
        post_migrate.connect(maybe_create_default_admin, sender=self)
        post_migrate.connect(create_local_miner_validator, sender=self)
