import logging
from decimal import Decimal

from django.conf import settings

from compute_horde_validator.validator.models import Miner

from .base import CollateralBase
from .types import BurnInputError, MinerCollateral, SlashedEvent


logger = logging.getLogger(__name__)


class Collateral(CollateralBase):
    def list_miners_with_sufficient_collateral(self, min_amount_wei: int) -> list[MinerCollateral]:
        threshold = min_amount_wei

        miners = Miner.objects.filter(collateral_wei__gte=Decimal(threshold))
        result: list[MinerCollateral] = [
            MinerCollateral(
                hotkey=m.hotkey,
                uid=m.uid,
                evm_address=m.evm_address,
                collateral_wei=int(m.collateral_wei),
            )
            for m in miners
        ]
        return result

    def slash_collateral(
        self,
        *,
        miner_hotkey: str | None = None,
        evm_address: str | None = None,
        amount_wei: int | None = None,
        url: str = "",
    ) -> SlashedEvent:
        raise NotImplementedError("Implemented in Phase 3")


_collateral_instance: Collateral | None = None


def collateral() -> Collateral:
    global _collateral_instance
    if _collateral_instance is None:
        _collateral_instance = Collateral()
    return _collateral_instance


