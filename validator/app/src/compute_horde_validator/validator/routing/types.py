import pydantic

from compute_horde_validator.validator.allowance.types import Miner, block_ids, reservation_id


class JobRoutingException(Exception):
    pass


class AllMinersBusy(JobRoutingException):
    pass


class JobRoute(pydantic.BaseModel):
    miner: Miner
    allowance_blocks: block_ids | None
    allowance_reservation_id: reservation_id | None
