import enum
from enum import Enum

import pydantic

from compute_horde.receipts import Receipt

from ..base_requests import BaseRequest, JobMixin
from ..em_protocol.executor_requests import JobErrorType
from ..executor_class import ExecutorClass
from ..utils import MachineSpecs


class RequestType(enum.Enum):
    V0AcceptJobRequest = "V0AcceptJobRequest"
    V0DeclineJobRequest = "V0DeclineJobRequest"
    V0ExecutorManifestRequest = "V0ExecutorManifestRequest"
    V0ExecutorReadyRequest = "V0ExecutorReadyRequest"
    V0ExecutorFailedRequest = "V0ExecutorFailedRequest"
    V0StreamingJobReadyRequest = "V0StreamingJobReadyRequest"
    V0StreamingJobNotReadyRequest = "V0StreamingJobNotReadyRequest"
    V0JobFailedRequest = "V0JobFailedRequest"
    V0JobFinishedRequest = "V0JobFinishedRequest"
    V0MachineSpecsRequest = "V0MachineSpecsRequest"
    GenericError = "GenericError"
    UnauthorizedError = "UnauthorizedError"


class ExecutorClassManifest(pydantic.BaseModel):
    # TODO: remove support for deprecated `int` executor class
    executor_class: ExecutorClass | int
    count: int


class ExecutorManifest(pydantic.BaseModel):
    executor_classes: list[ExecutorClassManifest]

    @property
    def total_count(self) -> int:
        return sum([x.count for x in self.executor_classes])


class BaseMinerRequest(BaseRequest):
    message_type: RequestType


class V0AcceptJobRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0AcceptJobRequest


class V0DeclineJobRequest(BaseMinerRequest, JobMixin):
    class Reason(Enum):
        NOT_SPECIFIED = "not_specified"
        BUSY = "busy"
        EXECUTOR_RESERVATION_FAILURE = "executor_reservation_failure"
        EXECUTOR_FAILURE = "executor_failure"
        VALIDATOR_BLACKLISTED = "validator_blacklisted"

    message_type: RequestType = RequestType.V0DeclineJobRequest
    reason: Reason = Reason.NOT_SPECIFIED
    receipts: list[Receipt] = pydantic.Field(default_factory=list)


class V0ExecutorReadyRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0ExecutorReadyRequest


class V0ExecutorFailedRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0ExecutorFailedRequest


# TODO: sign this with the miners wallet when sent to vali
class V0StreamingJobReadyRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0StreamingJobReadyRequest
    public_key: str
    ip: str
    port: int


class V0StreamingJobNotReadyRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0StreamingJobNotReadyRequest


class V0JobFailedRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0JobFailedRequest
    docker_process_exit_status: int | None = None
    docker_process_stdout: str  # TODO: add max_length
    docker_process_stderr: str  # TODO: add max_length
    error_type: JobErrorType | None = None
    error_detail: str | None = None


class V0JobFinishedRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0JobFinishedRequest
    docker_process_stdout: str  # TODO: add max_length
    docker_process_stderr: str  # TODO: add max_length
    artifacts: dict[str, str] | None = None


class V0MachineSpecsRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0MachineSpecsRequest
    specs: MachineSpecs


class V0ExecutorManifestRequest(BaseMinerRequest):
    message_type: RequestType = RequestType.V0ExecutorManifestRequest
    manifest: ExecutorManifest


class GenericError(BaseMinerRequest):
    message_type: RequestType = RequestType.GenericError
    details: str | None = None


class UnauthorizedErrorType(enum.Enum):
    TOKEN_TOO_OLD = "TOKEN_TOO_OLD"
    UNKNOWN_VALIDATOR = "UNKNOWN_VALIDATOR"
    VALIDATOR_INACTIVE = "VALIDATOR_INACTIVE"


class UnauthorizedError(BaseMinerRequest):
    message_type: RequestType = RequestType.UnauthorizedError
    code: UnauthorizedErrorType
    details: str | None = None
