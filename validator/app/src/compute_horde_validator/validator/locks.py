import enum

from django.db import connection
from django.db.utils import OperationalError



class LockType(enum.Enum):
    WEIGHT_SETTING = 1
    VALIDATION_SCHEDULING = 2
    TRUSTED_MINER_LOCK = 3
    ALLOWANCE_BLOCK_INJECTION = 4
    ALLOWANCE_FETCHING = 5



class Locked(Exception):
    pass


def get_advisory_lock(type_: LockType) -> None:
    """
    Obtain postgres advisory lock.
    Has to be executed in transaction.atomic context. Throws `Locked` if not able to obtain the lock. The lock
    will be released automatically after transaction.atomic ends.
    """
    cursor = connection.cursor()
    cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", [type_.value])
    unlocked = cursor.fetchall()[0][0]
    if not unlocked:
        raise Locked


class LockTimeout(Locked):
    """
    Exception raised when lock acquisition times out.
    """
    def __init__(self, lock_type: LockType, timeout_seconds: float):
        super().__init__(f"Failed to acquire lock {lock_type.name} within {timeout_seconds}s")
        self.lock_type = lock_type
        self.timeout_seconds = timeout_seconds


class Lock:
    def __init__(self, type_: LockType, timeout_seconds: float):
        self.type = type_
        self.timeout_seconds = timeout_seconds

    def __enter__(self):
        cursor = connection.cursor()
        cursor.execute("SET LOCAL statement_timeout = %s", [f"{self.timeout_seconds:.3f}s"])

        try:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", [self.type.value])
        except OperationalError as e:
            # Check if it's a statement timeout error
            if "statement timeout" in str(e).lower():
                raise LockTimeout(self.type, self.timeout_seconds) from e
            # Re-raise other operational errors
            raise

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Lock is automatically released when transaction ends
        pass
