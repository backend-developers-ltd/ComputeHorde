"""
Prometheus metrics for the allowance module.
All metrics use the 'validator_' prefix for easier grouping in Grafana.
"""

import functools
import time
from collections.abc import Callable
from typing import Any, TypeVar

import prometheus_client

# Prometheus metrics for timing booking operations
VALIDATOR_RESERVE_ALLOWANCE_DURATION = prometheus_client.Histogram(
    "reserve_allowance_duration",
    "Time spent reserving allowance",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    namespace="validator",
    unit="seconds",
)

VALIDATOR_UNDO_ALLOWANCE_RESERVATION_DURATION = prometheus_client.Histogram(
    "undo_allowance_reservation_duration",
    "Time spent undoing allowance reservation",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    namespace="validator",
    unit="seconds",
)

VALIDATOR_BLOCK_ALLOWANCE_PROCESSING_DURATION = prometheus_client.Histogram(
    "block_allowance_processing_duration",
    "Time spent processing block allowance calculations",
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, float("inf")),
    namespace="validator",
    unit="seconds",
)

VALIDATOR_ALLOWANCE_CHECKPOINT = prometheus_client.Gauge(
    "block_allowance_checkpoint",
    "Total allowance per miner-validator-executorClass triplet",
    labelnames=["miner_hotkey", "validator_hotkey", "executor_class"],
    namespace="validator",
    unit="seconds",
)

VALIDATOR_STAKE_SHARE_REPORT = prometheus_client.Gauge(
    "stake_share",
    "the fraction of stake for each validator",
    labelnames=["validator_hotkey"],
    namespace="validator",
    unit="fraction",
    multiprocess_mode="liveall",
)

VALIDATOR_MINER_MANIFEST_REPORT = prometheus_client.Gauge(
    "miner_manifest",
    "miner manifest",
    labelnames=["miner_hotkey", "executor_class"],
    namespace="validator",
    unit="count",
    multiprocess_mode="liveall",
)

VALIDATOR_BLOCK_DURATION = prometheus_client.Gauge(
    "block_duration",
    "block duration",
    namespace="validator",
    unit="seconds",
    multiprocess_mode="liveall",
)


F = TypeVar("F", bound=Callable[..., Any])


def timing_decorator(metric: prometheus_client.Histogram) -> Callable[[F], F]:
    """Decorator to measure execution time and observe it in a Prometheus histogram."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.time() - start_time
                metric.observe(duration)

        return wrapper

    return decorator
