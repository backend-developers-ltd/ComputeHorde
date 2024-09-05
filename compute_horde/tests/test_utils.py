import pytest
from freezegun import freeze_time

from compute_horde.utils import Timer


def test_timer_none():
    with freeze_time("2024-01-01 00:00:00"):
        timer = Timer()
        with pytest.raises(ValueError):
            timer.time_left()


def test_timer_timeout():
    with freeze_time("2024-01-01 00:00:00") as frozen_time:
        timer = Timer(timeout=300)
        frozen_time.move_to("2024-01-01 00:00:30")
        assert timer.passed_time() == 30.0
        assert timer.time_left() == 270.0
