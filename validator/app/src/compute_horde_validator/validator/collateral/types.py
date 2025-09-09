from pydantic import BaseModel


class CollateralException(Exception):
    pass


class SlashCollateralError(Exception):
    pass


class MinerCollateral(BaseModel):
    hotkey: str
    collateral_wei: int
