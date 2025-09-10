from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from hexbytes import HexBytes

from compute_horde_validator.validator import clean_me_up as clean_me_up_module
from compute_horde_validator.validator.collateral import default as default_module
from compute_horde_validator.validator.collateral import tasks as tasks_module
from compute_horde_validator.validator.collateral.default import Collateral
from compute_horde_validator.validator.collateral.types import SlashCollateralError
from compute_horde_validator.validator.models import Miner


class DummyFn:
    def __init__(self, *args: Any):
        self.args = args

    def build_transaction(self, *_args, **_kwargs):
        raise AssertionError("build_transaction should not be called in this test")


class DummyContractFunctions:
    def slashCollateral(self, *args: Any) -> DummyFn:
        return DummyFn(*args)


class DummyEth:
    def __init__(self):
        self.chain_id = 1
        self.gas_price = 123

    def contract(self, address: str, abi: Any):
        return SimpleNamespace(functions=DummyContractFunctions())

    def get_transaction_count(self, _address: str) -> int:
        return 1

    def send_raw_transaction(self, _raw: bytes) -> HexBytes:
        return HexBytes("0x01")

    def wait_for_transaction_receipt(
        self, _tx_hash: HexBytes, _timeout: int, _poll: int
    ) -> dict[str, Any]:
        return {"status": 1, "transactionHash": HexBytes("0x01")}


class DummyW3:
    def __init__(self):
        self.eth = DummyEth()

    def to_checksum_address(self, addr: str) -> str:
        return addr


@dataclass
class DummyBlock:
    number: int
    hash: str


class MockShieldedBittensor:
    def __init__(self, *args, **kwargs):
        self.subtensor = SimpleNamespace()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.fixture(autouse=True)
def _reset_caches(monkeypatch):
    default_module._get_private_key.cache_clear()
    default_module._get_collateral_abi.cache_clear()
    monkeypatch.setattr(default_module, "_cached_contract_address", None, raising=False)


@pytest.fixture
def set_config(monkeypatch):
    monkeypatch.setattr(
        default_module,
        "config",
        SimpleNamespace(DYNAMIC_COLLATERAL_SLASH_AMOUNT_WEI=1000),
        raising=False,
    )


@pytest.fixture  # type: ignore[misc]
def w3(monkeypatch: pytest.MonkeyPatch) -> DummyW3:
    w3 = DummyW3()
    monkeypatch.setattr(default_module, "get_web3_connection", lambda network: w3, raising=True)
    return w3


@pytest.fixture
def account_patch(monkeypatch):
    fake_acct = SimpleNamespace(address="0xacc", key=b"" * 32)
    monkeypatch.setattr(default_module, "Account", SimpleNamespace(from_key=lambda _k: fake_acct))
    return fake_acct


@pytest.fixture
def private_key_patch(monkeypatch):
    monkeypatch.setattr(Collateral, "_get_private_key", lambda self: "0xprivkey")


@pytest.fixture
def abi_patch(monkeypatch):
    monkeypatch.setattr(
        Collateral, "_get_collateral_abi", lambda self: [{"name": "slashCollateral"}]
    )


@pytest.fixture
def contract_address_patch(monkeypatch):
    async def _addr(self):
        return "0xcontract"

    monkeypatch.setattr(Collateral, "get_collateral_contract_address", _addr)


@pytest.fixture
def http_ok(monkeypatch):
    def _set(content: bytes) -> bytes:
        class _Resp:
            def __init__(self, c: bytes):
                self.content = c

            def raise_for_status(self):
                return None

        monkeypatch.setattr(
            default_module, "requests", SimpleNamespace(get=lambda url, timeout: _Resp(content))
        )
        return hashlib.md5(content).digest()

    return _set


@pytest.fixture
def capture_send(monkeypatch):
    captured: dict[str, Any] = {}

    def _install():
        def _send(_self, _w3, function, _account, **_):
            captured["args"] = function.args
            return HexBytes("0xabc")

        monkeypatch.setattr(Collateral, "_build_and_send_transaction", _send)
        return captured

    return _install


class TestListMinersWithSufficientCollateral:
    @pytest.mark.django_db(transaction=True)
    def test_filters_on_threshold_and_returns_plain_values(self):
        # Below threshold
        Miner.objects.create(hotkey="hk1", uid=1, evm_address="0x1", collateral_wei=Decimal("500"))
        Miner.objects.create(hotkey="hk2", uid=2, evm_address="0x2", collateral_wei=Decimal("999"))
        # At or above threshold
        Miner.objects.create(hotkey="hk3", uid=3, evm_address="0x3", collateral_wei=Decimal("1000"))
        Miner.objects.create(hotkey="hk4", uid=4, evm_address="0x4", collateral_wei=Decimal("2000"))

        out = Collateral().list_miners_with_sufficient_collateral(min_amount_wei=1000)

        hotkeys = {m.hotkey for m in out}
        assert hotkeys == {"hk3", "hk4"}

        assert all(isinstance(m.collateral_wei, int) and m.collateral_wei >= 1000 for m in out)


class TestSlashCollateral:
    @pytest.mark.asyncio
    @pytest.mark.django_db(transaction=True)
    async def test_slash_success_http_url_checksums_and_sends(
        self,
        set_config,
        w3,
        account_patch,
        private_key_patch,
        abi_patch,
        contract_address_patch,
        http_ok,
        capture_send,
        monkeypatch,
    ):
        miner = await Miner.objects.acreate(
            hotkey="hk-evm",
            uid=1,
            evm_address="0x1234567890123456789012345678901234567890",
        )

        md5 = http_ok(b"evidence-bytes")

        captured = capture_send()

        monkeypatch.setattr(
            w3.eth,
            "wait_for_transaction_receipt",
            lambda *_: {"status": 1, "transactionHash": HexBytes("0xabc")},
        )

        await Collateral().slash_collateral(
            miner_hotkey=miner.hotkey, url="https://example.com/proof"
        )

        miner_addr, amount, url, md5_passed = captured["args"]
        assert miner_addr == miner.evm_address
        assert amount == 1000
        assert url == "https://example.com/proof"
        assert md5_passed == md5

    @pytest.mark.asyncio
    @pytest.mark.django_db(transaction=True)
    async def test_slash_errors_when_amount_not_positive(
        self,
        w3,
        account_patch,
        private_key_patch,
        abi_patch,
        contract_address_patch,
        monkeypatch,
    ):
        miner = await Miner.objects.acreate(hotkey="hk", uid=1, evm_address="0xabc")
        monkeypatch.setattr(
            default_module, "config", SimpleNamespace(DYNAMIC_COLLATERAL_SLASH_AMOUNT_WEI=0)
        )

        with pytest.raises(SlashCollateralError, match="Slash amount must be greater than 0"):
            await Collateral().slash_collateral(miner_hotkey=miner.hotkey, url="https://u")

    @pytest.mark.asyncio
    @pytest.mark.django_db(transaction=True)
    async def test_slash_errors_when_no_contract_address(
        self,
        set_config,
        w3,
        account_patch,
        private_key_patch,
        abi_patch,
        monkeypatch,
    ):
        miner = await Miner.objects.acreate(hotkey="hk", uid=1, evm_address="0xabc")

        async def _addr_none(self):
            return None

        monkeypatch.setattr(Collateral, "get_collateral_contract_address", _addr_none)

        with pytest.raises(
            SlashCollateralError, match="Collateral contract address not configured"
        ):
            await Collateral().slash_collateral(miner_hotkey=miner.hotkey, url="https://u")

    @pytest.mark.asyncio
    @pytest.mark.django_db(transaction=True)
    async def test_slash_fails_when_receipt_status_zero(
        self,
        set_config,
        w3,
        account_patch,
        private_key_patch,
        abi_patch,
        contract_address_patch,
        http_ok,
        monkeypatch,
    ):
        miner = await Miner.objects.acreate(hotkey="hk", uid=1, evm_address="0xabc")

        http_ok(b"content")
        monkeypatch.setattr(
            Collateral, "_build_and_send_transaction", lambda *a, **k: HexBytes("0x01")
        )
        monkeypatch.setattr(w3.eth, "wait_for_transaction_receipt", lambda *_: {"status": 0})

        with pytest.raises(SlashCollateralError, match="transaction failed"):
            await Collateral().slash_collateral(miner_hotkey=miner.hotkey, url="https://u")

    @pytest.mark.asyncio
    @pytest.mark.django_db(transaction=True)
    async def test_slash_errors_when_miner_missing(
        self, set_config, w3, account_patch, private_key_patch, abi_patch
    ):
        with pytest.raises(SlashCollateralError, match="not found"):
            await Collateral().slash_collateral(miner_hotkey="does-not-exist", url="https://u")

    @pytest.mark.asyncio
    @pytest.mark.django_db(transaction=True)
    async def test_slash_errors_when_miner_has_no_evm(
        self, set_config, w3, account_patch, private_key_patch, abi_patch
    ):
        await Miner.objects.acreate(hotkey="hk", uid=1, evm_address=None)
        with pytest.raises(SlashCollateralError, match="has no associated EVM address"):
            await Collateral().slash_collateral(miner_hotkey="hk", url="https://u")

    @pytest.mark.asyncio
    @pytest.mark.django_db(transaction=True)
    async def test_slash_asserts_when_private_key_missing(
        self, set_config, w3, abi_patch, monkeypatch
    ):
        await Miner.objects.acreate(hotkey="hk", uid=1, evm_address="0xabc")
        monkeypatch.setattr(Collateral, "_get_private_key", lambda self: None)
        with pytest.raises(AssertionError, match="EVM private key not found"):
            await Collateral().slash_collateral(miner_hotkey="hk", url="https://u")


class TestGetCollateralContractAddress:
    @pytest.mark.asyncio
    async def test_returns_cached_value_when_available(self, monkeypatch):
        monkeypatch.setattr(default_module, "_cached_contract_address", "0xCACHED", raising=False)
        out = await Collateral().get_collateral_contract_address()
        assert out == "0xCACHED"

    @pytest.mark.asyncio
    async def test_reads_from_commitment_and_caches(self, monkeypatch):
        class _HK:
            ss58_address = "addr-ss58"

        class _Wallet:
            hotkey = _HK()

        monkeypatch.setattr(
            default_module,
            "settings",
            SimpleNamespace(
                BITTENSOR_WALLET=lambda: _Wallet(), BITTENSOR_NETWORK="testnet", BITTENSOR_NETUID=42
            ),
        )

        class _Commitments:
            async def get(self, _hotkey):
                return '{"contract": {"address": "0xfrom-chain"}}'

        class _Subnet:
            commitments = _Commitments()

        class _BT:
            def __init__(self, _):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                return None

            def subnet(self, _netuid):
                return _Subnet()

        monkeypatch.setattr(default_module, "turbobt", SimpleNamespace(Bittensor=_BT))

        out1 = await Collateral().get_collateral_contract_address()
        assert out1 == "0xfrom-chain"
        monkeypatch.setattr(
            default_module,
            "turbobt",
            SimpleNamespace(
                Bittensor=lambda *_: (_ for _ in ()).throw(AssertionError("should not hit network"))
            ),
        )
        out2 = await Collateral().get_collateral_contract_address()
        assert out2 == "0xfrom-chain"


class TestTaskHelpers:
    def test_get_miner_collateral_happy_path(self, monkeypatch):
        calls: dict[str, list[str]] = {"checksum": []}

        class _ContractFns:
            def collaterals(self, who):
                assert who == "0xMINER"
                return SimpleNamespace(call=lambda **kw: 5000)

        class _Eth:
            def contract(self, address, abi):
                assert address == "0xCONTRACT"
                assert isinstance(abi, list)
                return SimpleNamespace(functions=_ContractFns())

        class _W3:
            def __init__(self):
                self.eth = _Eth()

            def to_checksum_address(self, a: str) -> str:
                calls["checksum"].append(a)
                return {"0xcontract": "0xCONTRACT", "0xminer": "0xMINER"}[a.lower()]

        monkeypatch.setattr(
            default_module.Collateral,
            "_get_collateral_abi",
            lambda self: [{"name": "collaterals", "type": "function"}],
        )

        out = tasks_module.get_miner_collateral(
            _W3(),  # type: ignore[arg-type]
            "0xcontract",
            "0xminer",
            block_identifier=123,
        )
        assert out == 5000
        assert calls["checksum"] == ["0xcontract", "0xminer"]


class TestSyncCollaterals:
    @pytest.mark.django_db(transaction=True)
    def test_updates_evm_and_collateral_for_known_uids(self, monkeypatch):
        m1 = Miner.objects.create(hotkey="hk1", uid=1, evm_address=None, collateral_wei=Decimal(0))
        m2 = Miner.objects.create(hotkey="hk2", uid=2, evm_address=None, collateral_wei=Decimal(0))
        Miner.objects.create(
            hotkey="not-in-metagraph", uid=99, evm_address=None, collateral_wei=Decimal(0)
        )

        @dataclass
        class _Neuron:
            hotkey: str

        neurons = [_Neuron("hk1"), _Neuron("hk2")]
        block = DummyBlock(number=12345, hash="0xBLOCK")

        async def mock_get_metagraph_for_sync(bt):
            return (neurons, SimpleNamespace(), block)

        monkeypatch.setattr(tasks_module, "_get_metagraph_for_sync", mock_get_metagraph_for_sync)

        associations = {1: "0xA", 2: "0xB"}

        async def mock_get_evm_key_associations(**kwargs):
            return associations

        monkeypatch.setattr(tasks_module, "get_evm_key_associations", mock_get_evm_key_associations)

        monkeypatch.setattr(tasks_module, "get_web3_connection", lambda network: DummyW3())

        async def mock_get_collateral_contract_address(self):
            return "0xCONTRACT"

        monkeypatch.setattr(
            default_module.Collateral,
            "get_collateral_contract_address",
            mock_get_collateral_contract_address,
        )
        monkeypatch.setattr(
            tasks_module,
            "settings",
            SimpleNamespace(
                BITTENSOR_NETWORK="net", BITTENSOR_NETUID=1, DEFAULT_DB_ALIAS="default"
            ),
        )

        monkeypatch.setattr(clean_me_up_module, "ShieldedBittensor", MockShieldedBittensor)

        monkeypatch.setattr(
            tasks_module,
            "get_miner_collateral",
            lambda w3, caddr, who, block_identifier: 5000,
        )

        tasks_module.sync_collaterals.run(bittensor=SimpleNamespace(subtensor=SimpleNamespace()))

        m1.refresh_from_db()
        m2.refresh_from_db()
        assert (m1.evm_address, int(m1.collateral_wei)) == ("0xA", 5000)
        assert (m2.evm_address, int(m2.collateral_wei)) == ("0xB", 5000)

    @pytest.mark.django_db(transaction=True)
    def test_handles_get_collateral_errors(self, monkeypatch):
        m = Miner.objects.create(hotkey="hk1", uid=1, evm_address=None, collateral_wei=Decimal(0))

        @dataclass
        class _Neuron:
            hotkey: str

        neurons = [_Neuron("hk1")]
        block = DummyBlock(number=111, hash="0xH")

        async def mock_get_metagraph_for_sync(bt):
            return (neurons, SimpleNamespace(), block)

        monkeypatch.setattr(tasks_module, "_get_metagraph_for_sync", mock_get_metagraph_for_sync)

        async def mock_get_evm_key_associations(**kwargs):
            return {1: "0xA"}

        monkeypatch.setattr(tasks_module, "get_evm_key_associations", mock_get_evm_key_associations)
        monkeypatch.setattr(tasks_module, "get_web3_connection", lambda network: DummyW3())

        async def mock_get_collateral_contract_address(self):
            return "0xCONTRACT"

        monkeypatch.setattr(
            default_module.Collateral,
            "get_collateral_contract_address",
            mock_get_collateral_contract_address,
        )

        monkeypatch.setattr(clean_me_up_module, "ShieldedBittensor", MockShieldedBittensor)

        (
            monkeypatch.setattr(
                tasks_module,
                "get_miner_collateral",
                lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
            ),
        )
        monkeypatch.setattr(
            tasks_module,
            "settings",
            SimpleNamespace(
                BITTENSOR_NETWORK="net", BITTENSOR_NETUID=1, DEFAULT_DB_ALIAS="default"
            ),
        )

        tasks_module.sync_collaterals.run(bittensor=SimpleNamespace(subtensor=SimpleNamespace()))

        m.refresh_from_db()
        assert m.evm_address == "0xA"
        assert int(m.collateral_wei) == 0
