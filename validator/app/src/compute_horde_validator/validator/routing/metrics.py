"""
Prometheus metrics for the routing module.
All metrics use the 'validator_' prefix for easier grouping in Grafana.
"""

import prometheus_client

VALIDATOR_MINER_INCIDENT_REPORTED = prometheus_client.Counter(
    "miner_incident_reported",
    "Count of miner incidents reported by type",
    labelnames=["incident_type", "miner_hotkey", "executor_class"],
    namespace="validator",
)
