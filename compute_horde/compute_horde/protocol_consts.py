import enum
import sys

if sys.version_info >= (3, 11):  # noqa: UP036
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  #  noqa: UP035


class JobParticipantType(StrEnum):
    UNKNOWN = "unknown"  # TODO(post error propagation): remove this
    SDK = "sdk"
    FACILITATOR = "facilitator"
    VALIDATOR = "validator"
    MINER = "miner"
    EXECUTOR = "executor"

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]

    def __repr__(self):
        return self.value


class JobStatus(StrEnum):
    """
    Job status common between the validator, facilitator, and the SDK.
    """

    SENT = "sent"
    RECEIVED = "received"
    ACCEPTED = "accepted"

    EXECUTOR_READY = "executor_ready"
    STREAMING_READY = "streaming_ready"
    VOLUMES_READY = "volumes_ready"
    EXECUTION_DONE = "execution_done"

    COMPLETED = "completed"
    REJECTED = "rejected"
    FAILED = "failed"
    HORDE_FAILED = "horde_failed"

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]

    @classmethod
    def end_states(cls) -> set["JobStatus"]:
        return {cls.COMPLETED, cls.REJECTED, cls.FAILED}

    def is_in_progress(self) -> bool:
        return self not in self.end_states()

    def is_successful(self) -> bool:
        return self == self.COMPLETED

    def is_failed(self) -> bool:
        return self in {self.REJECTED, self.FAILED}


class JobStage(StrEnum):
    UNKNOWN = "unknown"
    # ↓ Facilitator, validator
    ACCEPTANCE = "acceptance"
    ROUTING = "routing"
    # ↓ Miner
    RESERVATION = "reservation"
    EXECUTOR_SPINUP = "executor_spinup"
    # ↓ Executor
    EXECUTOR_STARTUP = "executor_startup"
    VOLUME_DOWNLOAD = "volume_download"
    STREAMING_STARTUP = "streaming_startup"
    EXECUTION = "execution"
    RESULT_UPLOAD = "result_upload"
    CLOSURE = "closure"

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]

    def __repr__(self):
        return self.value


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
    GENERIC_EXECUTOR_FAILED = "generic_executor_failed"
    UPSTREAM_CONNECTION_ERROR = "upstream_connection_error"

    def __repr__(self):
        return self.value


class JobFailureReason(StrEnum):
    UNKNOWN = "unknown"
    TIMEOUT = "TIMEOUT"
    # TODO(post error propagation): security check failure is a horde failure, remove it
    SECURITY_CHECK = "SECURITY_CHECK"
    HUGGINGFACE_DOWNLOAD = "HUGGINGFACE_DOWNLOAD"
    NONZERO_EXIT_CODE = "NONZERO_EXIT_STATUS"

    def __repr__(self):
        return self.value


class JobRejectionReason(StrEnum):
    UNKNOWN = "unknown"
    BUSY = "busy"
    INVALID_SIGNATURE = "invalid_signature"
    NO_MINER_FOR_JOB = "no_miner_for_job"
    EXECUTOR_FAILURE = "executor_failure"
    MINER_BLACKLISTED = (
        "miner_blacklisted"  # This is a legacy thing - consumers don't select miners anymore
    )
    NOT_ENOUGH_TIME_IN_CYCLE = "not_enough_time_in_cycle"
    VALIDATOR_BLACKLISTED = "validator_blacklisted"
    UNSAFE_VOLUME = "unsafe_volume"

    def __repr__(self):
        return self.value
