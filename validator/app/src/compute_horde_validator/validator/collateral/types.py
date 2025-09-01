from pydantic import BaseModel


class CollateralException(Exception):
    pass


class BurnInputError(CollateralException):
    pass


class MinerCollateral(BaseModel):
    hotkey: str
    uid: int | None
    evm_address: str | None
    collateral_wei: int


class SlashedEvent(BaseModel):
    tx_hash: str
    block_number: int
    miner_address: str
    amount_wei: int
    url: str


