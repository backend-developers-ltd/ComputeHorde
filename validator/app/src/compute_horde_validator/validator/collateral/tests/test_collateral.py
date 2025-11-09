from __future__ import annotations

import hashlib
from typing import cast
from unittest.mock import Mock

import pytest
from django.conf import settings
from web3 import Web3, exceptions

from compute_horde_validator.validator.collateral.default import Collateral
from compute_horde_validator.validator.collateral.tasks import get_miner_collateral
from compute_horde_validator.validator.collateral.types import (
    CollateralException,
    NonceTooHighCollateralException,
    NonceTooLowCollateralException,
    ReplacementUnderpricedCollateralException,
    SlashCollateralError,
)
from compute_horde_validator.validator.models import Miner

from .helpers.contexts import (
    CollateralTaskHarness,
)
from .helpers.env import CollateralTestEnvironment
from .helpers.setup_helpers import (
    setup_collateral,
)

SLASH_URL = "https://example.com/proof"
pytestmark = pytest.mark.django_db(transaction=True)


class TestListMinersWithCollateral:
    def test_list_miners_with_sufficient_collateral_when_threshold_met_returns_expected_miners(
        self,
    ) -> None:
        m1 = Miner.objects.create(hotkey="hk1")
        m2 = Miner.objects.create(hotkey="hk2")
        m3 = Miner.objects.create(hotkey="hk3")
        m4 = Miner.objects.create(hotkey="hk4")

        with setup_collateral(
            miners=[m1, m2, m3, m4],
            collaterals_wei={"hk1": 500, "hk2": 999, "hk3": 1_000, "hk4": 2_000},
        ):
            with CollateralTestEnvironment():
                result = Collateral().list_miners_with_sufficient_collateral(min_amount_wei=1_000)

        hotkeys = {miner.hotkey for miner in result}
        assert hotkeys == {"hk3", "hk4"}
        assert all(miner.collateral_wei >= 1_000 for miner in result)


class TestSlashCollateral:
    def test_slash_collateral_when_request_succeeds_emits_transaction(self) -> None:
        miner_obj = Miner.objects.create(hotkey="hk-evm")

        with setup_collateral(
            miners=[miner_obj],
            uids={"hk-evm": 1},
            evm_addresses={"hk-evm": "0x1234567890123456789012345678901234567890"},
        ):
            with CollateralTestEnvironment() as env:
                miner = miner_obj
                evidence = b"evidence-bytes"

                env.set_http_response(evidence)
                env.collateral.slash_collateral(miner_hotkey=miner.hotkey, url=SLASH_URL)

        assert env.requested_urls == [SLASH_URL]
        expected_addr = "0x1234567890123456789012345678901234567890"
        expected_args = (
            expected_addr.upper(),
            1_000,
            SLASH_URL,
            hashlib.md5(evidence).digest(),
        )
        assert env.transaction_call == (expected_args, {"gas_limit": 200_000, "value": 0})

    def test_slash_collateral_when_amount_not_positive_raises(self) -> None:
        miner_obj = Miner.objects.create(hotkey="hk")

        with setup_collateral(
            miners=[miner_obj],
            uids={"hk": 1},
            evm_addresses={"hk": "0xabc"},
        ):
            with CollateralTestEnvironment() as env:
                env.set_slash_amount(0)

                with pytest.raises(
                    SlashCollateralError, match="Slash amount must be greater than 0"
                ):
                    env.collateral.slash_collateral(miner_hotkey="hk", url=SLASH_URL)

    def test_slash_collateral_when_contract_address_missing_raises(self) -> None:
        miner_obj = Miner.objects.create(hotkey="hk")

        with setup_collateral(
            miners=[miner_obj],
            uids={"hk": 1},
            evm_addresses={"hk": "0xabc"},
        ):
            with CollateralTestEnvironment(contract_address=None) as env:
                with pytest.raises(
                    SlashCollateralError,
                    match="Collateral contract address not configured",
                ):
                    env.collateral.slash_collateral(miner_hotkey="hk", url=SLASH_URL)

    def test_slash_collateral_when_receipt_status_zero_raises(self) -> None:
        miner_obj = Miner.objects.create(hotkey="hk")

        with setup_collateral(
            miners=[miner_obj],
            uids={"hk": 1},
            evm_addresses={"hk": "0xabc"},
        ):
            with CollateralTestEnvironment(receipt={"status": 0}) as env:
                with pytest.raises(
                    SlashCollateralError,
                    match="collateral slashing transaction failed",
                ):
                    env.collateral.slash_collateral(miner_hotkey="hk", url=SLASH_URL)

    def test_slash_collateral_when_miner_missing_raises(self) -> None:
        with CollateralTestEnvironment() as env:
            with pytest.raises(SlashCollateralError, match="not found"):
                env.collateral.slash_collateral(miner_hotkey="missing", url=SLASH_URL)

    def test_slash_collateral_when_miner_has_no_evm_address_raises(self) -> None:
        miner_obj = Miner.objects.create(hotkey="hk")

        with setup_collateral(
            miners=[miner_obj],
            uids={"hk": 1},
            evm_addresses={"hk": None},
        ):
            with CollateralTestEnvironment() as env:
                with pytest.raises(
                    SlashCollateralError,
                    match="has no associated EVM address",
                ):
                    env.collateral.slash_collateral(miner_hotkey="hk", url=SLASH_URL)

    def test_slash_collateral_when_private_key_missing_raises(self) -> None:
        miner_obj = Miner.objects.create(hotkey="hk")

        with setup_collateral(
            miners=[miner_obj],
            uids={"hk": 1},
            evm_addresses={"hk": "0xabc"},
        ):
            with CollateralTestEnvironment(private_key=None) as env:
                with pytest.raises(AssertionError, match="EVM private key not found"):
                    env.collateral.slash_collateral(miner_hotkey="hk", url=SLASH_URL)


class TestGetCollateralContractAddress:
    def test_get_collateral_contract_address_when_configured_returns_value(self) -> None:
        with CollateralTestEnvironment() as env:
            result = env.collateral._get_collateral_contract_address()

        assert result == "0xcontract"

    def test_get_collateral_contract_address_when_missing_returns_none(self) -> None:
        with CollateralTestEnvironment(contract_address=None) as env:
            result = env.collateral._get_collateral_contract_address()

        assert result is None


class TestGetMinerCollateral:
    def test_get_miner_collateral_when_call_succeeds_returns_value(self) -> None:
        abi = Collateral()._get_collateral_abi()
        with CollateralTestEnvironment(collateral_values={"0xminer": 123}) as env:
            value = get_miner_collateral(
                cast(Web3, env.web3),
                contract_address="0xcontract",
                miner_address="0xminer",
                block_identifier=321,
            )

        assert value == 123
        assert env.contract_log == [("0XCONTRACT", abi)]
        assert [c.miner_address for c in env.web3_calls] == ["0XMINER"]
        assert [c.block_identifier for c in env.web3_calls] == [321]


class TestSyncCollaterals:
    def test_sync_collaterals_when_associations_available_updates_miners(self) -> None:
        m1 = Miner.objects.create(hotkey="hk1")
        m2 = Miner.objects.create(hotkey="hk2")
        m3 = Miner.objects.create(hotkey="not-in-metagraph")

        with setup_collateral(
            miners=[m1, m2, m3],
            uids={"hk1": 1, "hk2": 2, "not-in-metagraph": 99},
            evm_addresses={"hk1": "0xORIG1", "hk2": "0xORIG2", "not-in-metagraph": "0xORIG3"},
            collaterals_wei={"hk1": 0, "hk2": 0, "not-in-metagraph": 0},
        ):
            with CollateralTestEnvironment(collateral_values={"0xA": 5_000, "0xB": 6_000}) as env:
                harness = CollateralTaskHarness(
                    env=env,
                    neurons=[Mock(hotkey="hk1"), Mock(hotkey="hk2")],
                    block_number=12345,
                    block_hash="0xBLOCK",
                    associations={1: "0xA", 2: "0xB"},
                )

                harness.run()

                miner1 = Miner.objects.get(hotkey="hk1")
                miner2 = Miner.objects.get(hotkey="hk2")
                other = Miner.objects.get(hotkey="not-in-metagraph")

                assert (miner1.evm_address, int(miner1.collateral_wei)) == ("0xA", 5_000)
                assert (miner2.evm_address, int(miner2.collateral_wei)) == ("0xB", 6_000)
                assert other.evm_address == "0xORIG3"
                assert env.fetch_log == [
                    {"netuid": settings.BITTENSOR_NETUID, "block_hash": "0xBLOCK"}
                ]
                assert [call.miner_address for call in env.web3_calls] == ["0XA", "0XB"]

    def test_sync_collaterals_when_collateral_call_fails_records_event(self) -> None:
        m1 = Miner.objects.create(hotkey="hk1")

        with setup_collateral(
            miners=[m1],
            uids={"hk1": 1},
            evm_addresses={"hk1": "0xORIG"},
            collaterals_wei={"hk1": 0},
        ):
            with CollateralTestEnvironment(collateral_values={"0xA": RuntimeError("boom")}) as env:
                harness = CollateralTaskHarness(
                    env=env,
                    neurons=[Mock(hotkey="hk1")],
                    block_number=111,
                    block_hash="0xHASH",
                    associations={1: "0xA"},
                )

                harness.run()

                miner = Miner.objects.get(hotkey="hk1")
                assert (miner.evm_address, int(miner.collateral_wei)) == ("0xA", 0)
                assert env.fetch_log == [
                    {"netuid": settings.BITTENSOR_NETUID, "block_hash": "0xHASH"}
                ]
                assert [call.miner_address for call in env.web3_calls] == ["0XA"]
                failure_events = [event for event in env.system_events.records if event.get("data")]
                assert failure_events and failure_events[-1]["data"]["miner_hotkey"] == "hk1"


class TestBuildAndSendTransaction:
    @pytest.fixture
    def w3(self) -> Mock:
        w3 = Mock()
        w3.eth = Mock()
        w3.eth.get_transaction_count.return_value = 0
        w3.eth.gas_price = 1
        w3.eth.chain_id = 1
        w3.eth.account = Mock()
        w3.eth.account.sign_transaction.return_value = Mock(raw_transaction=b"\x00")
        return w3

    @pytest.fixture
    def function(self) -> Mock:
        function = Mock()

        def _build_tx(params):
            function.built_with = dict(params)
            return {"to": "0xcontract", **params}

        function.build_transaction.side_effect = _build_tx
        return function

    @pytest.fixture
    def account(self) -> Mock:
        return Mock(address="0xabc", key=b"\x00" * 32)

    @pytest.mark.parametrize(
        "message,expected_exc",
        [
            ("replacement transaction underpriced", ReplacementUnderpricedCollateralException),
            ("nonce too low", NonceTooLowCollateralException),
            ("nonce too high", NonceTooHighCollateralException),
            ("unknown rpc failure", CollateralException),
        ],
    )
    def test_maps_web3_rpc_error_to_custom_collateral_exceptions(
        self,
        w3: Mock,
        function: Mock,
        account: Mock,
        message: str,
        expected_exc: type[Exception],
    ):
        w3.eth.send_raw_transaction.side_effect = exceptions.Web3RPCError(message=message)

        with pytest.raises(expected_exc):
            Collateral()._build_and_send_transaction(
                w3=w3, function=function, account=account, gas_limit=200_000
            )

    def test_reraises_non_web3_rpc_errors(self, w3: Mock, function: Mock, account: Mock):
        w3.eth.send_raw_transaction.side_effect = ValueError("Other Value Error")

        with pytest.raises(ValueError):
            Collateral()._build_and_send_transaction(
                w3=w3, function=function, account=account, gas_limit=200_000
            )
