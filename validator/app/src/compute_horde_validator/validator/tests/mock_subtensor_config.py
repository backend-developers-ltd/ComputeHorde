import os
import pathlib
import tempfile
import time
from unittest.mock import patch

from compute_horde_validator.validator.tests.helpers import MockHyperparameters, MockSubtensor

print("Applying patch")
PYTEST_RUN_ID = os.environ.get("PYTEST_RUN_ID")
if not PYTEST_RUN_ID:
    raise ValueError("Environment variable PYTEST_RUN_ID not set")

counter_file = (
    pathlib.Path(tempfile.gettempdir()) / f"mock_subtensor_reveal_counter_{PYTEST_RUN_ID}"
)

if not counter_file.exists():
    print("creating the counter file")
    counter_file.write_text("0")


def reveal_weights_side_effect():
    counter = int(counter_file.read_text())
    counter_file.write_text(str(counter + 1))
    match counter:
        case 0:
            time.sleep(30)
            return False, "Failed to succeed because i was sleeping"
        case 1:
            raise ValueError
        case 2:
            return False, "Failed to succeed"
        case 3:
            return True, ""
        case _:
            raise RuntimeError("This was not supposed to be called this many times")


patch(
    "bittensor.subtensor",
    lambda *args, **kwargs: MockSubtensor(
        mocked_reveal_weights=reveal_weights_side_effect,
        override_block_number=1100,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    ),
).__enter__()
