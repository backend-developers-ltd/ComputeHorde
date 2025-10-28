import functools
import time
from collections.abc import Callable
from typing import Any, TypeVar

import prometheus_client

VALIDATOR_FC_COMPONENT_STATE = prometheus_client.Gauge(
    "facilitator_client_component_state",
    "Current state of facilitator client components (1=running, 0=stopped)",
    labelnames=[
        "component"
    ],  # One of ConnectionManager, MessageManager, HeartbeatManager, JobRequestManager
    namespace="validator",
)

VALIDATOR_FC_COMPONENT_UPTIME = prometheus_client.Gauge(
    "facilitator_client_component_uptime",
    "Time in seconds since the component's run-forever loop was started",
    labelnames=[
        "component"
    ],  # One of ConnectionManager, MessageManager, HeartbeatManager, JobRequestManager
    namespace="validator",
    unit="seconds",
)

VALIDATOR_FC_TRANSPORT_LAYER_STATE = prometheus_client.Gauge(
    "facilitator_client_transport_layer_state",
    "Current state of the transport layer (1=connected, 0=disconnected)",
    namespace="validator",
)

VALIDATOR_FC_TRANSPORT_LAYER_EVENTS = prometheus_client.Counter(
    "facilitator_client_transport_layer_events_total",
    "Total number of connection events with transport layer",
    labelnames=["event"],  # One of success, transport_error, auth_error, unknown_error
    namespace="validator",
)

VALIDATOR_FC_TRANSPORT_LAYER_CONNECTION_DURATION = prometheus_client.Histogram(
    "facilitator_client_transport_layer_connection_duration",
    "Time spent connecting to transport layer",
    buckets=[
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.075,
        0.1,
        0.25,
        0.5,
        0.75,
        1,
        2,
        3,
        5,
        10,
        float("inf"),
    ],
    namespace="validator",
    unit="seconds",
)

VALIDATOR_FC_TRANSPORT_LAYER_AUTHENTICATION_DURATION = prometheus_client.Histogram(
    "facilitator_client_transport_layer_authentication_duration",
    "Time spent authenticating with transport layer",
    buckets=[
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.075,
        0.1,
        0.25,
        0.5,
        0.75,
        1,
        2,
        3,
        5,
        10,
        float("inf"),
    ],
    namespace="validator",
    unit="seconds",
)

VALIDATOR_FC_MESSAGE_QUEUE_LENGTH = prometheus_client.Gauge(
    "facilitator_client_message_queue_length",
    "Current number of messages waiting to be sent to the facilitator",
    namespace="validator",
)

VALIDATOR_FC_MESSAGES_SENT = prometheus_client.Counter(
    "facilitator_client_messages_sent_total",
    "Total number of messages sent to facilitator with the number of retries needed",
    labelnames=["message_type", "retries"],
    namespace="validator",
)

VALIDATOR_FC_MESSAGES_RECEIVED = prometheus_client.Counter(
    "facilitator_client_messages_received_total",
    "Total number of messages received from facilitator",
    labelnames=["message_type"],
    namespace="validator",
)

VALIDATOR_FC_MESSAGE_SEND_FAILURES = prometheus_client.Counter(
    "facilitator_client_message_send_failures_total",
    "Total number of failed message send attempts (all retries failed)",
    labelnames=["message_type"],
    namespace="validator",
)

VALIDATOR_FC_MESSAGE_SEND_DURATION = prometheus_client.Histogram(
    "facilitator_client_message_send_duration",
    "Time spent sending messages to facilitator",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2, 3, 5, 10],
    labelnames=["message_type"],
    namespace="validator",
    unit="seconds",
)


F = TypeVar("F", bound=Callable[..., Any])


def timing_decorator(metric: prometheus_client.Histogram) -> Callable[[F], F]:
    """
    Decorator to measure execution time and observe it in a Prometheus histogram.

    The duration is only measured if the function completes successfully.
    """

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.monotonic()
            output = await func(*args, **kwargs)
            metric.observe(time.monotonic() - start_time)
            return output

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.monotonic()
            output = func(*args, **kwargs)
            metric.observe(time.monotonic() - start_time)
            return output

        # Return appropriate wrapper based on whether function is async
        import asyncio

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator
