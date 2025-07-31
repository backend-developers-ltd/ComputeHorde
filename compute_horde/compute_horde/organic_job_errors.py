from pydantic import JsonValue

from compute_horde import protocol_consts


class JobRejected(Exception):
    def __init__(
        self,
        rejected_by: protocol_consts.JobParticipantType,
        reason: protocol_consts.JobRejectionReason,
        message: str = "no details provided",
        context: dict[str, JsonValue] | None = None,
    ) -> None:
        super().__init__(f"Job rejected ({rejected_by=}, {reason=}, {message=})")
        self.rejected_by = rejected_by
        self.reason = reason
        self.message = message
        self.context = context


class JobFailed(Exception):
    def __init__(
        self,
        stage: protocol_consts.JobStage,
        reason: protocol_consts.JobFailureReason,
        docker_process_exit_status: int | None = None,
        docker_process_stdout: str | None = None,
        docker_process_stderr: str | None = None,
        message: str = "no details provided",
        context: dict[str, JsonValue] | None = None,
    ) -> None:
        super().__init__(f"Job failed ({stage=}, {reason=}, {message=})")
        self.stage = stage
        self.docker_process_exit_status = docker_process_exit_status
        self.docker_process_stdout = docker_process_stdout
        self.docker_process_stderr = docker_process_stderr
        self.reason = reason
        self.message = message
        self.context = context


class HordeFailed(Exception):
    def __init__(
        self,
        reported_by: protocol_consts.JobParticipantType,
        stage: protocol_consts.JobStage,
        reason: protocol_consts.HordeFailureReason,
        message: str = "no details provided",
        context: dict[str, JsonValue] | None = None,
    ) -> None:
        super().__init__(f"Horde failure ({reported_by=}, {stage=}, {reason=}, {message=})")
        self.reported_by = reported_by
        self.stage = stage
        self.reason = reason
        self.message = message
        self.context = context
