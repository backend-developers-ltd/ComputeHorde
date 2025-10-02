"""
Prometheus metrics for the scoring module.
All metrics use the 'validator_' prefix for easier grouping in Grafana.
"""

import prometheus_client

VALIDATOR_ALLOWANCE_BLOCKS_PROCESSED = prometheus_client.Gauge(
    "allowance_blocks_processed",
    "Blocks accepted for spending in last batch run",
    labelnames=["validator_hotkey", "miner_hotkey", "executor_class", "result"],
    namespace="validator",
    multiprocess_mode="livemostrecent",
)

VALIDATOR_ALLOWANCE_SPENDING_ATTEMPTS = prometheus_client.Gauge(
    "allowance_spending_attempts",
    "Allowance spending attempts by result in last batch run",
    labelnames=["validator_hotkey", "miner_hotkey", "executor_class", "result"],
    namespace="validator",
    multiprocess_mode="livemostrecent",
)

VALIDATOR_ALLOWANCE_PAID_JOB_SCORES = prometheus_client.Gauge(
    "allowance_paid_job_scores",
    "Current allowance-based job scores by miner and executor class",
    labelnames=["miner_hotkey", "executor_class"],
    namespace="validator",
    multiprocess_mode="livemostrecent",
)
