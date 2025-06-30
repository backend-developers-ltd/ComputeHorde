from health_probe.choices import HealthSeverity


class BaseProbeException(Exception):
    """
    Base exception class for all probe exceptions.
    """
    pass


class ProbeImproperlyConfigured(BaseProbeException):
    """
    Raised when a probe fails due to its misconfiguration.
    """
    pass


class HealthProbeFailed(BaseProbeException):
    """
    Base class for all health probe failures.
    """
    default_message = "Probe has failed."
    default_severity = HealthSeverity.UNHEALTHY

    def __init__(self, msg: str = None, severity: HealthSeverity = None):
        self.msg = msg or self.default_message
        self.severity = severity or self.default_severity
        super().__init__(self.msg)


class HealthProbeFailedJobCreation(HealthProbeFailed):
    """
    Raised when the health probe fails to create a job.
    """
    default_message = "Probe has failed to create the job."


class HealthProbeFailedJobExecution(HealthProbeFailed):
    """
    Raised when the job created by the health probe fails to finish execution.
    """
    default_message = "Execution of the probe's job has not been finished."


class HealthProbeFailedJobRejected(HealthProbeFailed):
    """
    Raised when the job created by the health probe is rejected.
    """
    default_message = "Probe's job has been rejected."
    default_severity = HealthSeverity.BUSY


class HealthProbeFailedJobFailed(HealthProbeFailed):
    """
    Raised when the job created by the health probe finishes execution but with the failed status.
    """
    default_message = "Probe's job has ended with the failed status."