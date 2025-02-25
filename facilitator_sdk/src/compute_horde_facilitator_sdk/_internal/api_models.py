from typing import Annotated, Literal, NotRequired

import annotated_types
from compute_horde.base.output_upload import SingleFilePostUpload, SingleFilePutUpload
from compute_horde.base.volume import HuggingfaceVolume, SingleFileVolume, ZipUrlVolume
from typing_extensions import TypedDict

JOB_STATUS_TYPE = Literal["Failed", "Rejected", "Sent", "Accepted", "Completed"]


def is_in_progress(status: JOB_STATUS_TYPE) -> bool:
    return status in ("Sent", "Accepted")


class JobState(TypedDict, total=False):
    uuid: str
    status: JOB_STATUS_TYPE
    stdout: str
    output_download_url: str


class JobFeedback(TypedDict):
    """
    Represents feedback data for a job, detailing the job's execution and results.

    :Attributes:
        - **job_uuid**: job UUID
        - **result_correctness**
            The correctness of the job's result expressed as a float between 0.0 and 1.0.
            - 0.0 indicates 0% correctness (completely incorrect).
            - 1.0 indicates 100% correctness (completely correct).
        - **expected_duration** (*NotRequired[float]*):
            An optional field indicating the expected time in seconds for the job to complete.
            This can highlight if the job's execution was slower than expected, suggesting performance issues.
    """

    result_correctness: Annotated[float, annotated_types.Interval(ge=0.0, le=1.0)]
    expected_duration: NotRequired[Annotated[float, annotated_types.Gt(0.0)] | None]


SingleFileUpload = SingleFilePostUpload | SingleFilePutUpload

Volume = ZipUrlVolume | SingleFileVolume | HuggingfaceVolume
