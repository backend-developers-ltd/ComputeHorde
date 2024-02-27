import enum

from ..base_requests import BaseRequest, JobMixin


class RequestType(enum.Enum):
    V0AcceptJobRequest = 'V0AcceptJobRequest'
    V0DeclineJobRequest = 'V0DeclineJobRequest'
    V0ExecutorReadyRequest = 'V0ExecutorReadyRequest'
    V0ExecutorFailedRequest = 'V0ExecutorFailedRequest'
    V0JobFailedRequest = 'V0JobFailedRequest'
    V0JobFinishedRequest = 'V0JobFinishedRequest'
    GenericError = 'GenericError'
    UnauthorizedError = 'UnauthorizedError'


class BaseMinerRequest(BaseRequest):
    message_type: RequestType


class V0AcceptJobRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0AcceptJobRequest


class V0DeclineJobRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0DeclineJobRequest


class V0ExecutorReadyRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0ExecutorReadyRequest


class V0ExecutorFailedRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0ExecutorFailedRequest


class V0JobFailedRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0JobFailedRequest
    docker_process_exit_status: int | None
    docker_process_stdout: str  # TODO: add max_length
    docker_process_stderr: str  # TODO: add max_length


class V0JobFinishedRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0JobFinishedRequest
    docker_process_stdout: str  # TODO: add max_length
    docker_process_stderr: str  # TODO: add max_length


class GenericError(BaseMinerRequest):
    message_type: RequestType = RequestType.GenericError
    details: str | None = None


class UnauthorizedErrorType(enum.Enum):

    TOKEN_TOO_OLD = 'TOKEN_TOO_OLD'
    UNKNOWN_VALIDATOR = 'UNKNOWN_VALIDATOR'
    VALIDATOR_INACTIVE = 'VALIDATOR_INACTIVE'


class UnauthorizedError(BaseMinerRequest):
    message_type: RequestType = RequestType.UnauthorizedError
    code: UnauthorizedErrorType
    details: str | None = None
