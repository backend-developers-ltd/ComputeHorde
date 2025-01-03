import logging
import threading
import time

from django.apps import AppConfig
from django.conf import settings
from django.db.models.signals import post_migrate

from compute_horde_validator.validator.sentry import init_sentry
from compute_horde_validator.validator.stats_collector_client import StatsCollectorClient

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


def maybe_init_sentry_from_stats_collector():
    if settings.SENTRY_DSN:
        return

    def target():
        # Fetch DSN from stats collector
        logger.info("Fetching Sentry DSN from stats collector")
        dsn = None
        while True:
            try:
                client = StatsCollectorClient()
                dsn = client.get_sentry_dsn()
            except Exception as e:
                logger.error("Error while fetching Sentry DSN from stats collector: %s", e)

            if dsn:
                logger.info("Initializing Sentry with custom DSN")
                init_sentry(dsn, settings.ENV)
                return

            if settings.FETCH_SENTRY_DSN_RETRY_INTERVAL <= 0:
                return

            logger.info(
                "Fetching Sentry DSN: retrying in %.1fs", settings.FETCH_SENTRY_DSN_RETRY_INTERVAL
            )
            # No DSN or connection error, sleep and retry
            time.sleep(settings.FETCH_SENTRY_DSN_RETRY_INTERVAL)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    # Not waiting for thread to finish to not block the app start.
    # If an exception is raised in the thread, it will be ignored.


class ValidatorConfig(AppConfig):
    name = "compute_horde_validator.validator"

    def ready(self):
        post_migrate.connect(maybe_create_default_admin, sender=self)
        maybe_init_sentry_from_stats_collector()
