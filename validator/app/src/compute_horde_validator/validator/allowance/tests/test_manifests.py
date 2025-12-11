from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.allowance.utils.manifests import (
    parse_commitments_to_manifests,
)


class TestParseCommitmentsToManifests:
    def test_parse_commitments(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"<M:A3;S2>",
            "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty": b"<M:L1>",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 3
        assert (
            result[
                (
                    "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                    ExecutorClass.always_on__gpu_24gb,
                )
            ]
            == 3
        )
        assert (
            result[
                (
                    "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                    ExecutorClass.spin_up_4min__gpu_24gb,
                )
            ]
            == 2
        )
        assert (
            result[
                (
                    "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
                    ExecutorClass.always_on__llm__a6000,
                )
            ]
            == 1
        )

    def test_parse_commitments_with_other_data(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"https://example.com<M:A5>",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 1
        assert (
            result[
                (
                    "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                    ExecutorClass.always_on__gpu_24gb,
                )
            ]
            == 5
        )

    def test_parse_commitments_empty_envelope(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"<M:>",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 0

    def test_parse_commitments_no_envelope(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"https://example.com",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 0

    def test_parse_commitments_mixed_valid_invalid(self):
        commitments = {
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY": b"<M:A3>",
            "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty": b"invalid data",
            "5DAAnrj7VHTznn2AWBemMuyBwZWs6FNFjdyVXUeYum3PTXFy": b"\xff\xfe",
        }

        result = parse_commitments_to_manifests(commitments)

        assert len(result) == 1
        assert (
            result[
                (
                    "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                    ExecutorClass.always_on__gpu_24gb,
                )
            ]
            == 3
        )

    def test_parse_commitments_empty_dict(self):
        commitments: dict[str, bytes] = {}

        result = parse_commitments_to_manifests(commitments)

        assert result == {}
