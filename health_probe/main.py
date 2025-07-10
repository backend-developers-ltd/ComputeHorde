import asyncio
import logging

import bittensor
from compute_horde_sdk.v1 import ComputeHordeClient
from prometheus_client import start_http_server

from health_probe import settings, jobs
from health_probe.analyzer import PrometheusHealthProbeAnalyzer
from health_probe.choices import HealthSeverity
from health_probe.models import HealthProbeResults
from health_probe.probe import HealthProbe, ProbeConfig


def get_probing_interval(results: HealthProbeResults):
    mapping = {
        HealthSeverity.HEALTHY: settings.PROBING_INTERVAL_HEALTHY,
        HealthSeverity.BUSY: settings.PROBING_INTERVAL_BUSY,
        HealthSeverity.UNHEALTHY: settings.PROBING_INTERVAL_UNHEALTHY,
    }
    return mapping[results.severity]


async def main():
    wallet = bittensor.wallet(
        name=settings.BITTENSOR_WALLET_NAME,
        hotkey=settings.BITTENSOR_WALLET_HOTKEY_NAME,
        path=settings.BITTENSOR_WALLET_PATH,
    )
    compute_horde_client = ComputeHordeClient(
        hotkey=wallet.hotkey,
        compute_horde_validator_hotkey=settings.VALIDATOR_HOTKEY,
        facilitator_url=settings.FACILITATOR_URL,
    )
    config = ProbeConfig(
        client=compute_horde_client,
        job_spec=getattr(jobs, settings.JOB_SPEC_NAME),
        job_timeout=settings.JOB_TIMEOUT,
    )
    analyzer = PrometheusHealthProbeAnalyzer(HealthProbe(config=config), get_probing_interval)
    server, thread = start_http_server(
        port=settings.METRICS_SERVER_PORT,
        certfile=settings.METRICS_SERVER_CERTFILE,
        keyfile=settings.METRICS_SERVER_KEYFILE,
    )
    try:
        await analyzer.analyze()
    except Exception as e:
        server.shutdown()
        thread.join(timeout=5)
        raise e


if __name__ == "__main__":
    logging.basicConfig(level=settings.LOGGING_LEVEL)
    asyncio.run(main())
