import datetime
import logging
import pickle

import turbobt
from django.core.cache import cache
from pylon.v1 import Neuron

from compute_horde_validator.validator.allowance.types import ValidatorModel
from compute_horde_validator.validator.allowance.utils.supertensor import BaseCache

logger = logging.getLogger(__name__)


class DjangoCache(BaseCache):
    def __init__(self):
        self.cache_key_prefix = "supertensor_cache"
        self.cache_timeout = 10 * 60  # 10 minutes

    def _get_key(self, data_type: str, block_number: int) -> str:
        return f"{self.cache_key_prefix}:{data_type}:{block_number}"

    def put_neurons(self, block_number: int, neurons: list[Neuron]):
        key = self._get_key("neurons", block_number)
        pickled_data = pickle.dumps(neurons)
        cache.set(key, pickled_data, self.cache_timeout)

    def put_block_timestamp(self, block_number: int, timestamp: datetime.datetime):
        key = self._get_key("block_timestamp", block_number)
        # Pickle handles datetime objects automatically
        pickled_data = pickle.dumps(timestamp)
        cache.set(key, pickled_data, self.cache_timeout)

    def put_block_hash(self, block_number: int, block_hash: str):
        key = self._get_key("block_hash", block_number)
        cache.set(key, block_hash, self.cache_timeout)

    def get_neurons(self, block_number: int) -> list[Neuron] | None:
        key = self._get_key("neurons", block_number)
        pickled_data = cache.get(key)
        if pickled_data is None:
            return None
        try:
            unpickled: list[Neuron] = pickle.loads(pickled_data)
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

    def get_block_hash(self, block_number: int) -> str | None:
        key = self._get_key("block_hash", block_number)
        result: str | None = cache.get(key)
        return result

    def put_subnet_state(self, block_number: int, state: turbobt.subnet.SubnetState):
        key = self._get_key("subnet_state", block_number)
        try:
            pickled_data = pickle.dumps(state)
            cache.set(key, pickled_data, self.cache_timeout)
        except Exception:
            logger.error("Error serializing subnet state:", exc_info=True)

    def get_subnet_state(self, block_number: int) -> turbobt.subnet.SubnetState | None:
        key = self._get_key("subnet_state", block_number)
        pickled_data = cache.get(key)
        if pickled_data is None:
            return None
        try:
            unpickled: turbobt.subnet.SubnetState = pickle.loads(pickled_data)
            return unpickled
        except Exception:
            logger.error("Error deserializing subnet state:", exc_info=True)
            return None

    def put_validators(self, block_number: int, validators: list[ValidatorModel]):
        key = self._get_key("validators", block_number)
        try:
            pickled_data = pickle.dumps(validators)
            cache.set(key, pickled_data, self.cache_timeout)
        except Exception:
            logger.error("Error serializing validators:", exc_info=True)

    def get_validators(self, block_number: int) -> list[ValidatorModel] | None:
        key = self._get_key("validators", block_number)
        pickled_data = cache.get(key)
        if pickled_data is None:
            return None
        try:
            unpickled: list[ValidatorModel] = pickle.loads(pickled_data)
            return unpickled
        except Exception:
            logger.error("Error deserializing validators:", exc_info=True)
            return None

    def put_commitments(self, block_number: int, commitments: dict[str, bytes]):
        key = self._get_key("commitments", block_number)
        try:
            pickled_data = pickle.dumps(commitments)
            cache.set(key, pickled_data, self.cache_timeout)
        except Exception:
            logger.error("Error serializing commitments:", exc_info=True)

    def get_commitments(self, block_number: int) -> dict[str, bytes] | None:
        key = self._get_key("commitments", block_number)
        pickled_data = cache.get(key)
        if pickled_data is None:
            return None
        try:
            unpickled: dict[str, bytes] = pickle.loads(pickled_data)
            return unpickled
        except Exception:
            logger.error("Error deserializing commitments:", exc_info=True)
            return None
