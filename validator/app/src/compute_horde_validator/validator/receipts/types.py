from datetime import datetime

import pydantic
from compute_horde_core.executor_class import ExecutorClass


class TransferIsDisabled(Exception):
    pass


class JobSpendingInfo(pydantic.BaseModel):
    job_uuid: str
    validator_hotkey: str
    miner_hotkey: str
    executor_class: ExecutorClass
    executor_seconds_cost: int
    paid_with_blocks: list[int]
    started_at: datetime
