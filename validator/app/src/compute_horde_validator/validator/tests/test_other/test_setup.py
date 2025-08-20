"""
This test file is here always to indicate that everything was installed and the CI was able to run tests.
It should always pass as long as all dependencies are properly installed.
"""

from datetime import timedelta

import pytest
from django.utils.timezone import now
from freezegun import freeze_time


def test__setup(db, some):
    with freeze_time(now() - timedelta(days=1)):
        assert some == 1

    with pytest.raises(ZeroDivisionError):
        1 / 0
