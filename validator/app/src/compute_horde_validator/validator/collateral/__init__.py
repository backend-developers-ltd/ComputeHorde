from .default import collateral
from .types import MinerCollateral, SlashedEvent
from .base import CollateralBase

__all__ = [
    "collateral",
    "CollateralBase",
    "MinerCollateral",
    "SlashedEvent",
]


