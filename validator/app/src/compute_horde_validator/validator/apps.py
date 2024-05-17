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
            logger.warning("Not creating Admin user - please set DEFAULT_ADMIN_PASSWORD env variable")
        else:
            logger.info("Creating Admin user with DEFAULT_ADMIN_PASSWORD")
            User.objects.create_superuser(username=settings.DEFAULT_ADMIN_USERNAME, email=settings.DEFAULT_ADMIN_EMAIL, password=admin_password)


class ValidatorConfig(AppConfig):
    name = 'compute_horde_validator.validator'

    def ready(self):
        post_migrate.connect(maybe_create_default_admin, sender=self)
