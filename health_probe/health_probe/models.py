import dataclasses
from datetime import datetime

from health_probe.choices import HealthSeverity


@dataclasses.dataclass
class BaseProbeResults:
    start_time: datetime
    end_time: datetime


@dataclasses.dataclass
class HealthProbeResults(BaseProbeResults):
    severity: HealthSeverity