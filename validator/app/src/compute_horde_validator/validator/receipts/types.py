from dataclasses import dataclass
from datetime import datetime

from compute_horde_core.executor_class import ExecutorClass


class TransferIsDisabled(Exception):
    pass


@dataclass
class JobSpendingInfo:
    job_uuid: str
    validator_hotkey: str
    miner_hotkey: str
    executor_class: ExecutorClass
    executor_seconds_cost: int
    paid_with_blocks: list[int]
    started_at: datetime
