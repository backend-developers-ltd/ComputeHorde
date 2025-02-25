"""
Test setup tests.

This test file is here always to indicate that everything was installed and the CI was able to run tests.
It should always pass as long as all dependencies are properly installed.
"""

from datetime import datetime, timedelta

import pytest
from freezegun import freeze_time


def test__setup():
    with freeze_time(datetime.now() - timedelta(days=1)):
        pass

    with pytest.raises(ZeroDivisionError):
        1 / 0
