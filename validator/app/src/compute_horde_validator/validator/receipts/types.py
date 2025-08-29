from datetime import datetime

from pydantic import BaseModel


class TransferIsDisabled(Exception):
    pass


class FinishedJobInfo(BaseModel):
    validator_hotkey: str
    miner_hotkey: str
    job_run_time_us: int
    block_start_time: datetime | None
    block_ids: list[int]
