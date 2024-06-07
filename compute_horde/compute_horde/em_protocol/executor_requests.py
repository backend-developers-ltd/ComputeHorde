import enum

from ..base_requests import BaseRequest, JobMixin
from ..utils import MachineSpecs


class RequestType(enum.Enum):
    V0ReadyRequest = "V0ReadyRequest"
    V0FailedToPrepare = "V0FailedToPrepare"
    V0FinishedRequest = "V0FinishedRequest"
    V0FailedRequest = "V0FailedRequest"
    V0MachineSpecsRequest = "V0MachineSpecsRequest"
    GenericError = "GenericError"


class BaseExecutorRequest(BaseRequest):
    message_type: RequestType


class V0ReadyRequest(BaseExecutorRequest, JobMixin):
    message_type: RequestType = RequestType.V0ReadyRequest


class V0FailedToPrepare(BaseExecutorRequest, JobMixin):
    message_type: RequestType = RequestType.V0FailedToPrepare


class V0FailedRequest(BaseExecutorRequest, JobMixin):
    message_type: RequestType = RequestType.V0FailedRequest
    docker_process_exit_status: int | None = None
    timeout: bool
    docker_process_stdout: str  # TODO: add max_length
    docker_process_stderr: str  # TODO: add max_length


class V0MachineSpecsRequest(BaseExecutorRequest, JobMixin):
    message_type: RequestType = RequestType.V0MachineSpecsRequest
    specs: MachineSpecs


class V0FinishedRequest(BaseExecutorRequest, JobMixin):
    message_type: RequestType = RequestType.V0FinishedRequest
    docker_process_stdout: str  # TODO: add max_length
    docker_process_stderr: str  # TODO: add max_length


class GenericError(BaseExecutorRequest):
    message_type: RequestType = RequestType.GenericError
    details: str | None = None
