import logging

from django.apps import AppConfig

logger = logging.getLogger(__name__)


class ComputeHordeReceiptsConfig(AppConfig):
    name = "compute_horde.receipts"
    verbose_name = "Receipts"
    default_auto_field = "django.db.models.BigAutoField"
