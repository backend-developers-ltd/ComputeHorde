from __future__ import annotations

from contextlib import ExitStack
from typing import Any
from unittest.mock import Mock, patch

from hexbytes import HexBytes

from compute_horde_validator.validator.collateral import default as collateral_default
from compute_horde_validator.validator.collateral.default import Collateral
from compute_horde_validator.validator.tests.helpers import patch_constance

from .stubs import (
    HttpStub,
    SystemEventRecorder,
    Web3Call,
    Web3Stub,
    _BittensorWrapper,
)


class CollateralTestEnvironment:
    def __init__(
        self,
        *,
        slash_amount: int = 1_000,
        contract_address: str | None = "0xcontract",
        private_key: str | None = "0xprivate",
        http_bytes: bytes = b"",
        http_ok: bool = True,
        receipt: dict[str, Any] | None = None,
        collateral_values: dict[str, int | Exception] | None = None,
    ) -> None:
        self.slash_amount = slash_amount
        self.contract_address = contract_address
        self.private_key = private_key
        self.http_bytes = http_bytes
        self.http_ok = http_ok
        self.receipt_data = receipt or {"status": 1}
        self.collateral_values = {
            address.upper(): value for address, value in (collateral_values or {}).items()
        }
        self.requested_urls: list[str] = []
        self.transaction_call: tuple[tuple[Any, ...], dict[str, Any]] | None = None
        self.web3_calls: list[Web3Call] = []
        self.fetch_log: list[dict[str, Any]] = []
        self.system_events = SystemEventRecorder()
        self.contract_log: list[tuple[str, Any]] = []
        self._constance_overlay: dict[str, Any] | None = None

    def __enter__(self) -> CollateralTestEnvironment:
        self._stack = ExitStack()
        self.web3 = Web3Stub(self)
        self.http = HttpStub(self)
        self._stack.enter_context(
            patch(
                "compute_horde_validator.validator.collateral.default.requests.get",
                self.http,
            )
        )
        self._stack.enter_context(
            patch(
                "compute_horde_validator.validator.collateral.default.get_web3_connection",
                return_value=self.web3,
            )
        )
        self._stack.enter_context(
            patch.object(collateral_default, "_cached_contract_address", None)
        )
        self._config = Mock()
        self._config.DYNAMIC_COLLATERAL_SLASH_AMOUNT_WEI = self.slash_amount
        self._constance_overlay = {
            "DYNAMIC_COLLATERAL_SLASH_AMOUNT_WEI": self.slash_amount,
        }
        self._stack.enter_context(
            patch_constance(self._constance_overlay),
        )
        self._stack.enter_context(
            patch(
                "compute_horde_validator.validator.collateral.default.settings.BITTENSOR_WALLET",
                return_value=self._mock_wallet(),
            )
        )
        self._stack.enter_context(
            patch(
                "compute_horde_validator.validator.collateral.default.Account.from_key",
                side_effect=self._account_from_key,
            )
        )
        bittensor_wrapper = _BittensorWrapper(self)
        self._stack.enter_context(
            patch(
                "compute_horde_validator.validator.collateral.default.turbobt.Bittensor",
                new=bittensor_wrapper,
            )
        )
        self.collateral = Collateral()
        self._stack.enter_context(
            patch.object(
                self.collateral,
                "_get_private_key",
                side_effect=lambda: self.private_key,
            )
        )
        self._stack.enter_context(
            patch.object(
                self.collateral,
                "_build_and_send_transaction",
                side_effect=self._record_transaction,
            )
        )
        self._stack.enter_context(
            patch.object(
                self.collateral,
                "_get_collateral_abi",
                return_value=[{"name": "slashCollateral"}],
            )
        )
        return self

    def __exit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> None:
        self._stack.__exit__(exc_type, exc, tb)

    async def __aenter__(self) -> CollateralTestEnvironment:
        self.__enter__()
        return self

    async def __aexit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> None:
        self.__exit__(exc_type, exc, tb)

    def set_http_response(self, content: bytes, *, ok: bool = True) -> None:
        self.http_bytes = content
        self.http_ok = ok

    def set_receipt(self, receipt: dict[str, Any]) -> None:
        self.receipt_data = receipt

    def set_slash_amount(self, amount: int) -> None:
        self.slash_amount = amount
        self._config.DYNAMIC_COLLATERAL_SLASH_AMOUNT_WEI = amount
        if self._constance_overlay is not None:
            self._constance_overlay["DYNAMIC_COLLATERAL_SLASH_AMOUNT_WEI"] = amount

    def _record_transaction(
        self,
        _web3: Any,
        function: Any,
        _account: Any,
        gas_limit: int,
        *,
        value: int = 0,
    ) -> HexBytes:
        self.transaction_call = (function.args, {"gas_limit": gas_limit, "value": value})
        return HexBytes("0xdead")

    def _account_from_key(self, key: str | None) -> Mock:
        if key != self.private_key:
            raise AssertionError("Unexpected private key provided")
        return Mock(address="0xaccount", key=b"\x00" * 32)

    def _mock_wallet(self) -> Any:
        wallet = Mock()
        wallet.hotkey = Mock()
        wallet.hotkey.ss58_address = "stub-ss58"
        return wallet


__all__ = [
    "CollateralTestEnvironment",
    "Web3Call",
]
