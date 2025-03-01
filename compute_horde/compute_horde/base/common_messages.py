from typing import Literal, Annotated

from pydantic import BaseModel, Field

from compute_horde.em_protocol.executor_requests import JobErrorType
from compute_horde.utils import MachineSpecs


# NOTE:
# miner.ec - The executor consumer of a Miner.
# miner.vc - The validator consumer of a Miner.


class GenericError(BaseModel):
    message_type: Literal["GenericError"] = "GenericError"
    details: str | None = None


# executor -> miner.ec -> miner.vc -> validator
class ExecutorFailedToPrepare(BaseModel):
    message_type: Literal["ExecutorFailedToPrepare"] = "ExecutorFailedToPrepare"
    job_uuid: str  # NOT on miner.ec -> miner.vc
    executor_token: str  # ONLY on miner.ec -> miner.vc


# executor -> miner.ec -> miner.vc -> validator
class StreamingJobFailedToPrepare(BaseModel):
    message_type: Literal["StreamingJobFailedToPrepare"] = "StreamingJobFailedToPrepare"
    job_uuid: str  # NOT on miner.ec -> miner.vc
    executor_token: str  # ONLY on miner.ec -> miner.vc


# executor -> miner.ec -> miner.vc -> validator
class ExecutorReady(BaseModel):
    message_type: Literal["ExecutorReady"] = "ExecutorReady"
    job_uuid: str  # NOT on miner.ec -> miner.vc
    executor_token: str  # ONLY on miner.ec -> miner.vc


# signaling the streaming job is ready to accept connections from user
# executor -> miner.ec -> miner.vc -> validator
class StreamingJobReady(BaseModel):
    message_type: Literal["StreamingJobReady"] = "StreamingJobReady"
    job_uuid: str  # NOT on miner.ec -> miner.vc
    executor_token: str  # ONLY on miner.ec -> miner.vc
    public_key: str
    ip: str  # NOT on executor -> miner.ec
    port: int


# executor -> miner.ec -> miner.vc -> validator
class JobFailed(BaseModel):
    message_type: Literal["JobFailed"] = "JobFailed"
    job_uuid: str
    docker_process_exit_status: int | None = None
    docker_process_stdout: str
    docker_process_stderr: str
    error_type: JobErrorType | None = None
    error_detail: str | None = None
    timeout: bool  # ONLY on executor -> miner.ec


# executor -> miner.ec -> miner.vc -> validator
class JobFinished(BaseModel):
    message_type: Literal["JobFinished"] = "JobFinished"
    job_uuid: str
    docker_process_stdout: str
    docker_process_stderr: str
    artifacts: dict[str, str] | None = None


# executor -> miner.ec -> miner.vc -> validator
class MachineSpecsRequest(BaseModel):
    message_type: Literal["MachineSpecsRequest"] = "MachineSpecsRequest"
    job_uuid: str
    specs: MachineSpecs


ExecutorToMinerMessage = Annotated[
    ExecutorFailedToPrepare
    | StreamingJobFailedToPrepare
    | ExecutorReady
    | StreamingJobReady
    | JobFailed
    | JobFinished
    | MachineSpecsRequest,
    Field(discriminator="message_type"),
]
