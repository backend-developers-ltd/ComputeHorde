from celery import shared_task

from .block_cache import cache_current_block


@shared_task()
def update_block_cache():
    cache_current_block()
