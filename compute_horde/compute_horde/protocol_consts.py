import enum
import sys
from enum import Enum

if sys.version_info >= (3, 11):  # noqa: UP036
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  #  noqa: UP035


class JobParticipantType(Enum):
    SDK = "sdk"
    FACILITATOR = "facilitator"
    VALIDATOR = "validator"
    MINER = "miner"
    EXECUTOR = "executor"

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]


class JobStage(Enum):
    NOT_SPECIFIED = "not_specified"
    # ↓ Facilitator, validator
    ACCEPTANCE = "acceptance"
    ROUTING = "routing"
    # ↓ Miner
    RESERVATION = "reservation"
    EXECUTOR_SPINUP = "executor_spinup"
    # ↓ Executor
    EXECUTOR_STARTUP = "executor_startup"
    STREAMING_STARTUP = "streaming_startup"
    VOLUME_DOWNLOAD = "volume_download"
    EXECUTION = "execution"
    RESULT_UPLOAD = "result_upload"
    CLOSURE = "closure"

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]


class MinerFailureReason(enum.Enum):
    """
    Legacy vali/miner/executor failure reasons
    Pulled from the organic job miner client
    TODO: Explode this into horde/job failure reasons
    """

    MINER_CONNECTION_FAILED = enum.auto()
    INITIAL_RESPONSE_TIMED_OUT = enum.auto()
    EXECUTOR_READINESS_RESPONSE_TIMED_OUT = enum.auto()
    VOLUMES_TIMED_OUT = enum.auto()
    VOLUMES_FAILED = enum.auto()
    EXECUTION_TIMED_OUT = enum.auto()
    EXECUTION_FAILED = enum.auto()
    FINAL_RESPONSE_TIMED_OUT = enum.auto()
    JOB_DECLINED = enum.auto()
    EXECUTOR_FAILED = enum.auto()
    STREAMING_JOB_READY_TIMED_OUT = enum.auto()
    JOB_FAILED = enum.auto()
    STREAMING_FAILED = enum.auto()


class HordeFailureReason(StrEnum):
    UNKNOWN = "unknown"
    UNCAUGHT_EXCEPTION = "uncaught_exception"
    STREAMING_SETUP_FAILED = "streaming_setup_failed"
    JOB_IMAGE_MISSING = "job_image_missing"
    SECURITY_CHECK_FAILED = "security_check_failed"
    GENERIC_STREAMING_SETUP_FAILED = "generic_streaming_setup_failed"
    UPSTREAM_CONNECTION_ERROR = "upstream_connection_error"


class JobFailureReason(StrEnum):
    UNKNOWN = "unknown"
    TIMEOUT = "TIMEOUT"
    SECURITY_CHECK = "SECURITY_CHECK"
    HUGGINGFACE_DOWNLOAD = "HUGGINGFACE_DOWNLOAD"
    NONZERO_EXIT_CODE = "NONZERO_EXIT_STATUS"


class JobRejectionReason(StrEnum):
    UNKNOWN = "unknown"
    BUSY = "busy"
    INVALID_SIGNATURE = "invalid_signature"
    NO_MINER_FOR_JOB = "no_miner_for_job"
    EXECUTOR_FAILURE = "executor_failure"
    VALIDATOR_BLACKLISTED = "validator_blacklisted"
