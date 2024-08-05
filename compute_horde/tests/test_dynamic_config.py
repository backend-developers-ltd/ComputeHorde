from argparse import Namespace

import pytest
from freezegun import freeze_time

from compute_horde.dynamic_config import sync_dynamic_config


def test_dynamic_config__non_dynamic_key_skipped(mocked_responses):
    # arrange
    config_url = "http://127.0.0.1:8000/config.json"
    mocked_responses.get(
        config_url,
        json={
            "RANDOM_KEY": {
                "description": "...",
                "items": [{"value": 1}],
            },
            "DYNAMIC_KEY": {
                "description": "...",
                "items": [{"value": 2}],
            },
        },
    )
    namespace = Namespace()

    # act
    sync_dynamic_config(config_url, namespace)

    # assert
    assert vars(namespace) == {"DYNAMIC_KEY": 2}


@pytest.mark.parametrize(
    ("frozen_time", "expected_value"),
    (
        ("2024-01-02T00:00:00.000Z", 2),
        ("2024-01-03T00:01:01.000Z", 3),
        ("2024-01-04T01:01:01.000Z", 4),
    ),
)
def test_dynamic_config__correct_time_is_picked(mocked_responses, frozen_time, expected_value):
    # arrange
    config_url = "http://127.0.0.1:8000/config.json"
    mocked_responses.get(
        config_url,
        json={
            "DYNAMIC_KEY": {
                "description": "...",
                "items": [
                    {
                        "value": i,
                        "effective_from": f"2024-01-0{i}T00:00:00.000Z",
                    }
                    for i in range(1, 6)
                ],
            }
        },
    )
    namespace = Namespace()

    # act
    with freeze_time(frozen_time):
        sync_dynamic_config(config_url, namespace)

    # assert
    assert vars(namespace) == {"DYNAMIC_KEY": expected_value}
