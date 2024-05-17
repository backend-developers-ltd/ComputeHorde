import enum
import random

import msgpack
from channels_redis.core import RedisChannelLayer


def default(value):
    if isinstance(value, enum.Enum):
        return value.value
    return value


class ECRedisChannelLayer(RedisChannelLayer):
    """EC stands for "enum-compatible" """
    def serialize(self, message):
        """
        Serializes message to a byte string.
        """
        value = msgpack.packb(message, use_bin_type=True, default=default)
        if self.crypter:
            value = self.crypter.encrypt(value)

        # As we use an sorted set to expire messages we need to guarantee uniqueness, with 12 bytes.
        random_prefix = random.getrandbits(8 * 12).to_bytes(12, "big")
        return random_prefix + value
