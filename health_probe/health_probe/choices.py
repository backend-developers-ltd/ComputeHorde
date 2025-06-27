from enum import IntEnum


class HealthSeverity(IntEnum):
    HEALTHY = 0
    BUSY = 1
    UNHEALTHY = 2

    @classmethod
    def get_default_probing_intervals(cls):
        return {
            cls.HEALTHY: 60 * 5,
            cls.BUSY: 60 * 2,
            cls.UNHEALTHY: 60 * 2,
        }
