from django.db import connection


class LockType:
    WEIGHT_SETTING = 1
    VALIDATION_SCHEDULING = 2
    TRUSTED_MINER_LOCK = 3


class Locked(Exception):
    pass


def get_advisory_lock(type_: LockType) -> None:
    """
    Obtain postgres advisory lock.
    Has to be executed in transaction.atomic context. Throws `Locked` if not able to obtain the lock. The lock
    will be released automatically after transaction.atomic ends.
    """
    cursor = connection.cursor()
    cursor.execute("SELECT pg_try_advisory_lock(%s)", [type_])
    unlocked = cursor.fetchall()[0][0]
    if not unlocked:
        raise Locked
