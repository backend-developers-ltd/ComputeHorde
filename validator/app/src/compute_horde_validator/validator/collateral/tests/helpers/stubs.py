from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

if TYPE_CHECKING:
    from .env import CollateralTestEnvironment


@dataclass
class Web3Call:
    miner_address: str
    block_identifier: int | None


class HttpStub:
    def __init__(self, facade: CollateralTestEnvironment) -> None:
        self._facade = facade

    def __call__(self, url: str, *, timeout: int | float | None = None) -> Any:
        self._facade.requested_urls.append(url)

        class _Response:
            def __init__(self, data: bytes, ok: bool) -> None:
                self.content = data
                self._ok = ok

            def raise_for_status(self) -> None:
                if not self._ok:
                    raise RuntimeError("HTTP error")

        return _Response(self._facade.http_bytes, self._facade.http_ok)


class Web3Stub:
    def __init__(self, facade: CollateralTestEnvironment) -> None:
        self._facade = facade
        self.eth = Mock()
        self.eth.contract = self._contract
        self.eth.wait_for_transaction_receipt = self._wait_for_receipt

    def to_checksum_address(self, value: str) -> str:
        return value.upper()

    def _contract(self, *, address: str, abi: Any) -> Any:
        checksum = self.to_checksum_address(address)
        self._facade.contract_log.append((checksum, abi))

        def collaterals(miner_address: str) -> Any:
            miner_checksum = self.to_checksum_address(miner_address)

            def call(*, block_identifier: int | None = None) -> int:
                self._facade.web3_calls.append(
                    Web3Call(miner_address=miner_checksum, block_identifier=block_identifier)
                )
                value = self._facade.collateral_values.get(miner_checksum, 0)
                if isinstance(value, Exception):
                    raise value
                assert isinstance(value, int)
                return value

            obj = Mock()
            obj.call = call
            return obj

        def slash_collateral(
            miner_address: str,
            amount_wei: int,
            url: str,
            checksum: bytes,
        ) -> Any:
            tx = Mock()
            tx.args = (miner_address, amount_wei, url, checksum)
            return tx

        functions = Mock()
        functions.collaterals = collaterals
        functions.slashCollateral = slash_collateral

        contract = Mock()
        contract.functions = functions
        return contract

    def _wait_for_receipt(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return self._facade.receipt_data


class SystemEventRecorder:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def create(self, **data: Any) -> None:
        self.records.append(data)


class BittensorStub:
    def __init__(self, facade: CollateralTestEnvironment) -> None:
        self._facade = facade
        self.subtensor = Mock()

    async def __aenter__(self) -> BittensorStub:
        return self

    async def __aexit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> None:
        return None

    def subnet(self, _netuid: int) -> Any:
        facade = self._facade

        subnet = Mock()
        subnet.commitments = Mock()
        subnet.commitments.get = _CommitmentGetter(facade).get
        return subnet


class _CommitmentGetter:
    def __init__(self, facade: CollateralTestEnvironment) -> None:
        self._facade = facade

    async def get(self, _hotkey: str) -> str | None:
        if self._facade.contract_address is None:
            return None
        return json.dumps({"contract": {"address": self._facade.contract_address}})


class _BittensorWrapper:
    def __init__(self, facade: CollateralTestEnvironment) -> None:
        self._facade = facade

    def __call__(self, *_args: Any, **_kwargs: Any) -> BittensorStub:
        return BittensorStub(self._facade)

    def subnet(self, instance: BittensorStub, netuid: int) -> Any:
        return instance.subnet(netuid)


__all__ = [
    "Web3Call",
    "HttpStub",
    "Web3Stub",
    "SystemEventRecorder",
    "BittensorStub",
    "_BittensorWrapper",
]
