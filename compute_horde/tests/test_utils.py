import pytest
from freezegun import freeze_time

from compute_horde.utils import Timer, canonical_json, sign_blob, verify_blob_signature


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


def test_canonical_json_depends_only_on_content():
    assert canonical_json({"b": [1, {"d": None, "c": "x"}], "a": 1.5}) == (
        '{"a":1.5,"b":[1,{"c":"x","d":null}]}'
    )
    assert canonical_json({"a": 1, "b": 2}) == canonical_json({"b": 2, "a": 1})


def test_verify_blob_signature(keypair):
    signature = sign_blob(keypair, "blob")

    assert verify_blob_signature(keypair.ss58_address, "blob", signature)
    assert not verify_blob_signature(keypair.ss58_address, "other blob", signature)
    assert not verify_blob_signature(keypair.ss58_address, "blob", "gibberish")
    assert not verify_blob_signature("not an ss58 address", "blob", signature)
