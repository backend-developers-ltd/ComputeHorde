from pydantic import BaseModel


class CollateralException(Exception):
    pass


class NonceTooLowCollateralException(CollateralException):
    pass


class NonceTooHighCollateralException(CollateralException):
    pass


class ReplacementUnderpricedCollateralException(CollateralException):
    pass


class SlashCollateralError(Exception):
    pass


class MinerCollateral(BaseModel):
    hotkey: str
    collateral_wei: int
