from dataclasses import dataclass
from typing import Any

from hexbytes import HexBytes
from pydantic import BaseModel


class CollateralException(Exception):
    pass


class SlashCollateralError(Exception):
    pass


class MinerCollateral(BaseModel):
    hotkey: str
    uid: int | None
    evm_address: str | None
    collateral_wei: int


@dataclass
class SlashedEvent:
    event: str
    logIndex: int
    transactionIndex: int
    transactionHash: HexBytes
    address: str
    blockHash: HexBytes
    blockNumber: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SlashedEvent":
        return cls(
            event=data["event"],
            logIndex=data["logIndex"],
            transactionIndex=data["transactionIndex"],
            transactionHash=data["transactionHash"],
            address=data["address"],
            blockHash=data["blockHash"],
            blockNumber=data["blockNumber"],
        )
