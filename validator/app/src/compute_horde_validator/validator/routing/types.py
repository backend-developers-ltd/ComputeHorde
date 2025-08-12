import pydantic

from compute_horde_validator.validator.allowance.types import Miner, reservation_id


class JobRoutingException(Exception):
    pass


class NoMinerForExecutorType(JobRoutingException):
    pass


class AllMinersBusy(JobRoutingException):
    pass


class MinerIsBlacklisted(JobRoutingException):
    pass


class NotEnoughTimeInCycle(JobRoutingException):
    pass


class NoMinerWithEnoughAllowance(JobRoutingException):
    pass


class JobRoute(pydantic.BaseModel):
    miner: Miner
    allowance_reservation_id: reservation_id | None
