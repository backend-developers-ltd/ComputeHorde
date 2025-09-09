import datetime
import re
from contextlib import contextmanager
from re import Pattern
from typing import Any

import prometheus_client.metrics

from compute_horde_validator.validator.models import SystemEvent

from ...models.allowance.internal import Block, BlockAllowance


def get_histogram_metric_values(metric):
    """Extract count and sum values from histogram samples."""
    count = 0
    sum_value = 0
    for sample_family in metric.collect():
        for sample in sample_family.samples:
            if sample.name.endswith("_count"):
                count = sample.value
            elif sample.name.endswith("_sum"):
                sum_value = sample.value
    return count, sum_value


@contextmanager
def assert_metric_observed(metric: prometheus_client.Histogram, operation_name: str):
    """Context manager to assert that a Prometheus metric was observed and print the measurement."""

    # Get initial metric values
    count_before, samples_before = get_histogram_metric_values(metric)

    yield

    # Get metric values after operation
    count_after, samples_after = get_histogram_metric_values(metric)
    duration = samples_after - samples_before

    print(f"{operation_name} took {duration:.6f}s")

    # Assert that the metric was observed
    assert count_after > count_before, f"{operation_name} metric should have been observed"


class LenientFloat(float):
    rel_tol = 1e-6
    abs_tol = 1e-12

    def __eq__(self, other):
        if isinstance(other, int | float):
            return abs(self - other) <= max(self.rel_tol * max(abs(self), abs(other)), self.abs_tol)
        return super().__eq__(other)


LF = LenientFloat


def allowance_dict(allowances: list[tuple[str, float]]) -> dict[str, float]:
    return {hk: LF(val) for hk, val in allowances}


def inject_blocks_with_allowances(n_blocks):
    """
    Find the earliest block with BlockAllowance records, then inject N blocks
    before it with the same BlockAllowance records for each new block.
    """

    # Find the earliest block that has BlockAllowance records
    earliest_block_with_allowances = (
        BlockAllowance.objects.select_related("block")
        .order_by("block__block_number")  # Assuming Block has block_number field
        .first()
    )
    assert earliest_block_with_allowances

    earliest_block = earliest_block_with_allowances.block
    earliest_block_number = earliest_block.block_number  # Adjust field name as needed
    earliest_creation_timestamp = earliest_block.creation_timestamp

    new_blocks = []
    block_duration = datetime.timedelta(seconds=12)

    for i in range(n_blocks):
        new_block_number = earliest_block_number - n_blocks + i

        blocks_before_earliest = n_blocks - i
        creation_timestamp = earliest_creation_timestamp - (blocks_before_earliest * block_duration)
        end_timestamp = creation_timestamp + block_duration

        new_blocks.append(
            Block(
                block_number=new_block_number,
                creation_timestamp=creation_timestamp,
                end_timestamp=end_timestamp,
            )
        )

    created_blocks = Block.objects.bulk_create(new_blocks)
    print(f"Created {len(created_blocks)} new blocks using ORM")

    from django.db import connection

    with connection.cursor() as cursor:
        sql = f"""
            WITH RECURSIVE block_numbers AS (
                SELECT %s AS block_num
                UNION ALL
                SELECT block_num + 1
                FROM block_numbers
                WHERE block_num < %s
            )
            INSERT INTO {BlockAllowance._meta.db_table} 
            (block_id, allowance, miner_ss58, validator_ss58, executor_class, invalidated_at_block, allowance_booking_id)
            SELECT 
                bn.block_num,
                ba.allowance,
                ba.miner_ss58,
                ba.validator_ss58,
                ba.executor_class,
                ba.invalidated_at_block,
                ba.allowance_booking_id
            FROM {BlockAllowance._meta.db_table} ba
            CROSS JOIN block_numbers bn
            WHERE ba.block_id = %s
        """

        start_block = earliest_block_number - n_blocks
        end_block = earliest_block_number - 1

        cursor.execute(sql, [start_block, end_block, earliest_block_number])
        total_allowances_created = cursor.rowcount

    print(f"Inserted {total_allowances_created} new BlockAllowance records using SQL")


@contextmanager
def assert_system_events(specs: list[dict[str, Any]]):
    before = list(SystemEvent.objects.values_list("id", flat=True))
    yield
    after = list(SystemEvent.objects.exclude(id__in=before))
    for spec in specs:
        for event in after:
            if all(getattr(event, key) == value for key, value in spec.items()):
                break
        else:
            raise AssertionError(f"{spec} not found in system events: {after}")


class Matcher:
    """A helper class for regex matching in test assertions."""

    def __init__(self, pattern: str | Pattern[str]):
        if isinstance(pattern, str):
            self.pattern = re.compile(pattern)
        else:
            self.pattern = pattern

    def __eq__(self, other) -> bool:
        if not isinstance(other, str):
            return False
        return bool(self.pattern.match(other))

    def __repr__(self) -> str:
        return f"Matcher({self.pattern.pattern!r})"


def get_metric_values_by_remaining_labels(
    metric: prometheus_client.metrics.MetricWrapperBase, filter_labels: dict[str, str]
) -> dict[str, float]:
    """
    From a Prometheus metric and a dict of labelname->labelvalue to filter by,
    return a mapping from the remaining label values (excluding the filtered ones),
    concatenated together with no separator, to the metric values.

    Notes:
    - Only samples whose labels match the provided filter_labels exactly for the given
      label names are considered.
    - The remaining label values are concatenated in the order defined by the metric's
      labelnames (metric._labelnames), with the filtered label names removed. If the
      metric has no labelnames or all are filtered out, the resulting key will be an
      empty string "".
    - For Histogram/Summary metrics, multiple sample names (e.g., _count, _sum, buckets)
      may be present. This helper is primarily intended for Gauge/Counter. We restrict
      to samples whose name matches the family name collected from the metric to avoid
      _count/_sum artifacts.
    """
    result: dict[str, float] = {}

    remaining_names = [ln for ln in getattr(metric, "_labelnames", []) if ln not in filter_labels]

    for family in metric.collect():
        family_name = family.name  # fully-qualified (e.g., namespace_name)
        for sample in family.samples:
            # Skip helper samples like _count/_sum for Histograms/Summaries
            if sample.name != family_name:
                continue

            # sample.labels is a dict[str, str]
            labels = sample.labels or {}

            # Check filter match
            match = True
            for k, v in filter_labels.items():
                if labels.get(k) != v:
                    match = False
                    break
            if not match:
                continue

            key = "".join(labels.get(name, "") for name in remaining_names)
            result[key] = float(sample.value)

    return result


def normalize_stake_dict(dict_: dict[str, int | float]) -> dict[str, float]:
    return {k: LF(v / sum(dict_.values())) for k, v in dict_.items()}
