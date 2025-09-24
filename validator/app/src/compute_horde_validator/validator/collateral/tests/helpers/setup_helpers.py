from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from decimal import Decimal
from typing import Any

from asgiref.sync import sync_to_async

from compute_horde_validator.validator.models import Miner


@contextmanager
def setup_collateral(
    *,
    miners: list[Miner],
    uids: dict[str, int] | None = None,
    evm_addresses: dict[str, str | None] | None = None,
    collaterals_wei: dict[str, int] | None = None,
) -> Iterator[list[Miner]]:
    previous: dict[int, dict[str, Any]] = {}
    hotkey_to_miner: dict[str, Miner] = {m.hotkey: m for m in miners}

    def _collect_updates(hotkey: str) -> dict[str, Any]:
        updates: dict[str, Any] = {}
        if uids and hotkey in uids:
            updates["uid"] = uids[hotkey]
        if evm_addresses and hotkey in evm_addresses:
            updates["evm_address"] = evm_addresses[hotkey]
        if collaterals_wei and hotkey in collaterals_wei:
            updates["collateral_wei"] = Decimal(collaterals_wei[hotkey])
        return updates

    try:
        for hotkey, miner in hotkey_to_miner.items():
            updates = _collect_updates(hotkey)
            if not updates:
                continue
            previous[miner.pk] = {
                "uid": miner.uid,
                "evm_address": miner.evm_address,
                "collateral_wei": miner.collateral_wei,
            }
            Miner.objects.filter(pk=miner.pk).update(**updates)
            miner.refresh_from_db()
        yield miners
    finally:
        if previous:
            for miner in miners:
                if miner.pk not in previous:
                    continue
                Miner.objects.filter(pk=miner.pk).update(**previous[miner.pk])
                miner.refresh_from_db()


@asynccontextmanager
async def async_setup_collateral(
    *,
    miners: list[Miner],
    uids: dict[str, int] | None = None,
    evm_addresses: dict[str, str | None] | None = None,
    collaterals_wei: dict[str, int] | None = None,
) -> AsyncIterator[list[Miner]]:
    ctx = setup_collateral(
        miners=miners,
        uids=uids,
        evm_addresses=evm_addresses,
        collaterals_wei=collaterals_wei,
    )
    await sync_to_async(ctx.__enter__, thread_sensitive=True)()
    try:
        yield miners
    finally:
        await sync_to_async(ctx.__exit__, thread_sensitive=True)(None, None, None)


__all__ = [
    "setup_collateral",
    "async_setup_collateral",
]
