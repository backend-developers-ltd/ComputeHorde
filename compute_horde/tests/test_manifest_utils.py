import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde.manifest_utils import (
    EXECUTOR_CLASS_TO_SHORT,
    extract_manifest_payload,
    format_manifest_commitment,
    merge_manifest,
    parse_commitment_string,
)


class TestExecutorClassToShortMapping:
    def test_all_executor_classes_are_in_mapping(self):
        missing_classes = []
        for executor_class in ExecutorClass:
            if executor_class not in EXECUTOR_CLASS_TO_SHORT:
                missing_classes.append(executor_class)

        assert missing_classes == [], (
            f"The following ExecutorClass values are missing from EXECUTOR_CLASS_TO_SHORT: "
            f"{missing_classes}. Please add them to the mapping."
        )

    def test_all_short_codes_are_unique(self):
        short_codes = list(EXECUTOR_CLASS_TO_SHORT.values())
        unique_short_codes = set(short_codes)

        assert len(short_codes) == len(unique_short_codes), (
            f"EXECUTOR_CLASS_TO_SHORT contains duplicate short codes. "
            f"All values: {short_codes}, duplicates: "
            f"{[code for code in unique_short_codes if short_codes.count(code) > 1]}"
        )


class TestFormatManifestCommitment:
    def test_format_manifest_commitment(self):
        manifest = {
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
            ExecutorClass.always_on__gpu_24gb: 3,
        }
        result = format_manifest_commitment(manifest)
        assert result == "A3;S2"

    def test_format_manifest_commitment_empty(self):
        manifest = {}
        result = format_manifest_commitment(manifest)
        assert result == ""

    def test_format_manifest_commitment_single_class(self):
        manifest = {ExecutorClass.always_on__llm__a6000: 5}
        result = format_manifest_commitment(manifest)
        assert result == "L5"

    def test_format_manifest_commitment_all_classes(self):
        manifest = {
            ExecutorClass.spin_up_4min__gpu_24gb: 1,
            ExecutorClass.always_on__gpu_24gb: 2,
            ExecutorClass.always_on__llm__a6000: 3,
            ExecutorClass.always_on__test: 5,
        }
        result = format_manifest_commitment(manifest)
        expected = "A2;L3;S1;T5"
        assert result == expected

    def test_format_manifest_commitment_too_long(self):
        long_manifest = {
            ExecutorClass.always_on__gpu_24gb: 999999999999999999999999999999999999999999999999,
            ExecutorClass.always_on__llm__a6000: 999999999999999999999999999999999999999999999999,
            ExecutorClass.spin_up_4min__gpu_24gb: 999999999999999999999999999999999999999999999999,
        }
        with pytest.raises(ValueError) as exc_info:
            format_manifest_commitment(long_manifest)
        assert "exceeds maximum" in str(exc_info.value)


class TestParseCommitmentString:
    def test_parse_commitment_string(self):
        commitment = "A3;S2"
        result = parse_commitment_string(commitment)
        expected = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
        }
        assert result == expected

    def test_parse_commitment_string_empty(self):
        result = parse_commitment_string("")
        assert result == {}

    def test_parse_commitment_string_invalid_format(self):
        result = parse_commitment_string("A")
        assert result == {}

    def test_parse_commitment_string_invalid_executor_class(self):
        result = parse_commitment_string("X6")
        assert result == {}

    def test_parse_commitment_string_invalid_count(self):
        result = parse_commitment_string("Aabc")
        assert result == {}

    def test_parse_commitment_string_with_spaces(self):
        commitment = " A3 ; S2 "
        result = parse_commitment_string(commitment)
        expected = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
        }
        assert result == expected

    def test_parse_commitment_string_partial_invalid(self):
        commitment = "A3;invalid-class"
        result = parse_commitment_string(commitment)
        expected = {ExecutorClass.always_on__gpu_24gb: 3}
        assert result == expected


class TestEnvelopeHandling:
    def test_extract_manifest_payload_with_envelope(self):
        commitment = "https://example.com/data<M:A2> extra"
        other_data, payload = extract_manifest_payload(commitment)
        assert payload == "A2"
        assert "https://example.com/data" in other_data
        assert "extra" in other_data

    def test_extract_manifest_payload_empty_envelope(self):
        commitment = "<M:>"
        other_data, payload = extract_manifest_payload(commitment)
        assert payload == ""
        assert other_data == ""

    def test_merge_manifest_empty_commitment(self):
        result = merge_manifest("", "A2")
        assert result == "<M:A2>"

    def test_merge_manifest_no_existing_envelope(self):
        existing = "https://example.com/data"
        result = merge_manifest(existing, "A3")
        assert result == "https://example.com/data<M:A3>"

    def test_merge_manifest_replaces_existing_envelope(self):
        existing = "https://example.com/data<M:A2>"
        result = merge_manifest(existing, "A3")
        assert result == "https://example.com/data<M:A3>"

    def test_merge_manifest_preserves_other_data(self):
        existing = "start<M:O1> end"
        result = merge_manifest(existing, "N2")
        assert "start" in result
        assert "end" in result
        assert "<M:N2>" in result
        assert "O1" not in result

    def test_merge_manifest_empty_manifest_string(self):
        existing = "https://example.com/data<M:A2>"
        result = merge_manifest(existing, "")
        assert result == "https://example.com/data<M:>"

    def test_merge_manifest_round_trip(self):
        original = '{"contract": "0xabc"}<M:A5>'
        other_data, payload = extract_manifest_payload(original)

        merged = merge_manifest(original, "A7")

        new_other_data, new_payload = extract_manifest_payload(merged)
        assert new_other_data == other_data
        assert new_payload == "A7"


class TestRoundTrip:
    def test_round_trip_multiple_classes(self):
        original = {
            ExecutorClass.always_on__gpu_24gb: 3,
            ExecutorClass.spin_up_4min__gpu_24gb: 2,
            ExecutorClass.always_on__llm__a6000: 1,
        }
        commitment = format_manifest_commitment(original)
        parsed = parse_commitment_string(commitment)
        assert original == parsed
