import asyncio
from abc import ABC, abstractmethod
from collections.abc import Callable, AsyncGenerator

from prometheus_client import Counter, Gauge

from health_probe.models import BaseProbeResults, HealthProbeResults
from health_probe.probe import Probe


class ProbeAnalyzer(ABC):

    def __init__(self, probe: Probe, probing_interval_func: Callable[[BaseProbeResults], float]):
        self.probe = probe
        self.probing_interval_func = probing_interval_func

    @abstractmethod
    async def translate_results(self, results: BaseProbeResults): ...

    async def launch(self) -> AsyncGenerator[BaseProbeResults]:
        """
        Generator that let the estimation happen periodically on demand,
        but not sooner than the designated probing interval.
        """
        while True:
            results = await self.probe.estimate()
            timer = asyncio.create_task(asyncio.sleep(self.probing_interval_func(results)))
            yield results
            await timer

    async def analyze(self):
        async for probe_result in self.launch():
            await self.translate_results(probe_result)


class PrometheusHealthProbeAnalyzer(ProbeAnalyzer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.severity_counter = Counter(
            "health_severity",
            "Status of the network's health",
            ["level"]
        )
        self.launch_timestamp_gauge = Gauge(
            "health_severity_probe_launch_timestamp",
            "Timestamp when the last health probe was launched",
        )

    async def translate_results(self, results: HealthProbeResults) -> None:
        self.severity_counter.labels(level=results.severity).inc()
        self.launch_timestamp_gauge.set(results.start_time.timestamp())
