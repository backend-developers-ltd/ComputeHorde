import logging
from abc import ABC, abstractmethod
from functools import cache

from django.conf import settings
from django.core.cache import caches
from django.core.exceptions import ImproperlyConfigured

logger = logging.getLogger(__name__)


class TransferCheckpointBackend(ABC):
    @abstractmethod
    async def get(self, key: str) -> int: ...

    @abstractmethod
    async def set(self, key: str, checkpoint: int) -> None: ...


class DjangoCacheTransferCheckpointBackend(TransferCheckpointBackend):
    def __init__(self):
        if not getattr(settings, "RECEIPT_TRANSFER_CHECKPOINT_CACHE", ""):
            raise ImproperlyConfigured(
                "Required settings.py setting missing: RECEIPT_TRANSFER_CHECKPOINT_CACHE"
            )

        self.cache = caches[settings.RECEIPT_TRANSFER_CHECKPOINT_CACHE]  # type: ignore

    async def get(self, key: str) -> int:
        try:
            return int(await self.cache.aget(key, 0))
        except TypeError:
            logger.error(
                f"Django cache contained non-integer checkpoint value for {key}. Defaulting to 0."
            )
            return 0

    async def set(self, key: str, checkpoint: int) -> None:
        await self.cache.aset(key, str(checkpoint))


@cache
def checkpoint_backend() -> TransferCheckpointBackend:
    return DjangoCacheTransferCheckpointBackend()
