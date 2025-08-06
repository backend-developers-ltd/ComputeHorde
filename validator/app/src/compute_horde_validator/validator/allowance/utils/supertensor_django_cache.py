import datetime
import logging
import pickle

import turbobt
from django.core.cache import cache

from compute_horde_validator.validator.allowance.utils.supertensor import BaseCache

logger = logging.getLogger(__name__)


class DjangoCache(BaseCache):
    def __init__(self):
        self.cache_key_prefix = "supertensor_cache"
        self.cache_timeout = 10 * 60  # 10 minutes

    def _get_key(self, data_type: str, block_number: int) -> str:
        return f"{self.cache_key_prefix}:{data_type}:{block_number}"

    def put_neurons(self, block_number: int, neurons: list[turbobt.Neuron]):
        key = self._get_key("neurons", block_number)
        # Serialize the entire list of objects into a byte stream
        for neuron in neurons:
            neuron.subnet = None  # type_check: ignore
            neuron.prometheus_info = None  # type_check: ignore
            # TODO: fix this with something more clever. currently these neurons don't have the full
            # capabilities of neurons but that shouldn't be a biggie rn
        pickled_data = pickle.dumps(neurons)
        cache.set(key, pickled_data, self.cache_timeout)

    def put_block_timestamp(self, block_number: int, timestamp: datetime.datetime):
        key = self._get_key("block_timestamp", block_number)
        # Pickle handles datetime objects automatically
        pickled_data = pickle.dumps(timestamp)
        cache.set(key, pickled_data, self.cache_timeout)

    def get_neurons(self, block_number: int) -> list[turbobt.Neuron] | None:
        key = self._get_key("neurons", block_number)
        pickled_data = cache.get(key)
        if pickled_data is None:
            return None
        # Deserialize the byte stream back into the original list of objects
        try:
            unpickled: list[turbobt.Neuron] = pickle.loads(pickled_data)
            return unpickled
        except Exception:
            logger.error("Error deserializing neurons:", exc_info=True)
            return None

    def get_block_timestamp(self, block_number: int) -> datetime.datetime | None:
        key = self._get_key("block_timestamp", block_number)
        pickled_data = cache.get(key)
        if pickled_data is None:
            return None
        # Deserialize the byte stream back into a datetime object
        try:
            unpickled: datetime.datetime = pickle.loads(pickled_data)
            return unpickled
        except Exception:
            logger.error("Error deserializing block timestamp:", exc_info=True)
            return None
