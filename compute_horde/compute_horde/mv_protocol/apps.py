import logging

from django.apps import AppConfig

logger = logging.getLogger(__name__)


class ComputeHordeMVProtocolConfig(AppConfig):
    name = "compute_horde.mv_protocol"
    verbose_name = "Miner-Validator communication"
    default_auto_field = "django.db.models.BigAutoField"
