import logging

from celery import shared_task

from .block_cache import cache_current_block

logger = logging.getLogger(__name__)


@shared_task()
def update_block_cache():
    logger.info("Updating block cache")
    cache_current_block()
