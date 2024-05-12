from django.db import connection


class LockType:
    WEIGHT_SETTING = 1


class Locked(Exception):
    pass


def get_weight_setting_lock():
    """
    Obtain postgres lock for setting weights.
    Has to be executed in transaction.atomic context. Throws `Locked` if not able to obtain the lock. The lock
    will be released automatically after transaction.atomic ends.
    """
    cursor = connection.cursor()
    cursor.execute("SELECT pg_try_advisory_lock(%s)", [LockType.WEIGHT_SETTING])
    unlocked = cursor.fetchall()[0][0]
    if not unlocked:
        raise Locked()
