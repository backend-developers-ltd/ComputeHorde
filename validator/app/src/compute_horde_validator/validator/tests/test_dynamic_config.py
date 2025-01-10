from compute_horde_validator.validator.dynamic_config import parse_system_event_limits
from compute_horde_validator.validator.models import SystemEvent


def test_parse_system_event_limits_empty():
    assert parse_system_event_limits("") == {}


def test_parse_system_event_limits_single():
    type = SystemEvent.EventType.LLM_PROMPT_SAMPLING
    subtype = SystemEvent.EventSubType.SUCCESS
    count = 42
    assert parse_system_event_limits(f"{type.value},{subtype.value},{count}") == {
        (type, subtype): count,
    }


def test_parse_system_event_limits_multiple():
    type1 = SystemEvent.EventType.LLM_PROMPT_SAMPLING
    subtype1 = SystemEvent.EventSubType.SUCCESS
    count1 = 42
    type2 = SystemEvent.EventType.LLM_PROMPT_SAMPLING
    subtype2 = SystemEvent.EventSubType.SUCCESS
    count2 = 72
    assert parse_system_event_limits(
        f"{type1.value},{subtype1.value},{count1};{type2.value},{subtype2.value},{count2}"
    ) == {
        (type1, subtype1): count1,
        (type2, subtype2): count2,
    }


def test_parse_system_event_limits_malformed_skipped():
    assert parse_system_event_limits("malformed") == {}


def test_parse_system_event_limits_partial_malformed_skipped():
    type = SystemEvent.EventType.LLM_PROMPT_SAMPLING
    subtype = SystemEvent.EventSubType.SUCCESS
    count = 42
    assert parse_system_event_limits(f"malformed;{type.value},{subtype.value},{count}") == {
        (type, subtype): count,
    }


def test_parse_system_event_limits_missing_type_skipped():
    type = "missing"
    subtype = SystemEvent.EventSubType.SUCCESS
    count = 42
    assert parse_system_event_limits(f"malformed;{type},{subtype.value},{count}") == {}


def test_parse_system_event_limits_missing_subtype_skipped():
    type = SystemEvent.EventType.LLM_PROMPT_SAMPLING
    subtype = "missing"
    count = 42
    assert parse_system_event_limits(f"malformed;{type.value},{subtype},{count}") == {}


def test_parse_system_event_limits_invalid_count_skipped():
    type = SystemEvent.EventType.LLM_PROMPT_SAMPLING
    subtype = SystemEvent.EventSubType.SUCCESS
    count = "wat?"
    assert parse_system_event_limits(f"malformed;{type.value},{subtype.value},{count}") == {}
