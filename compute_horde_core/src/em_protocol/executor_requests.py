import enum

from ..base_requests import BaseRequest, JobMixin
from ..utils import MachineSpecs


class RequestType(enum.Enum):
    V0ReadyRequest = "V0ReadyRequest"
    V0FailedToPrepare = "V0FailedToPrepare"
    V0StreamingJobReadyRequest = "V0StreamingJobReadyRequest"
    V0StreamingJobFailedToPrepareRequest = "V0StreamingJobFailedToPrepareRequest"
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


# signaling the streaming job is ready to accept connections from user
class V0StreamingJobReadyRequest(BaseExecutorRequest, JobMixin):
    message_type: RequestType = RequestType.V0StreamingJobReadyRequest
    public_key: str
    port: int


class V0StreamingJobFailedToPrepareRequest(BaseExecutorRequest, JobMixin):
    message_type: RequestType = RequestType.V0StreamingJobFailedToPrepareRequest


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
    artifacts: dict[str, str] | None = None


class GenericError(BaseExecutorRequest):
    message_type: RequestType = RequestType.GenericError
    details: str | None = None
