from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
from hexbytes import HexBytes

from compute_horde_validator.validator.collateral.default import Collateral
from compute_horde_validator.validator.collateral.tasks import (
    get_evm_key_associations,
    get_miner_collateral,
    sync_collaterals,
)
from compute_horde_validator.validator.collateral.types import (
    SlashedEvent,
)
from compute_horde_validator.validator.models import Miner


class TestCollateral:
    @pytest.mark.django_db(transaction=True)
    def test_list_miners_with_sufficient_collateral_mixed_miners(self, db):
        """Test filtering with mixed miners above and below threshold."""
        # Miners below threshold
        Miner.objects.create(
            hotkey="hotkey1",
            uid=1,
            evm_address="0x1234567890123456789012345678901234567890",
            collateral_wei=Decimal("500"),
        )
        Miner.objects.create(
            hotkey="hotkey2",
            uid=2,
            evm_address="0x2345678901234567890123456789012345678901",
            collateral_wei=Decimal("999"),
        )

        # Miners at or above threshold
        Miner.objects.create(
            hotkey="hotkey3",
            uid=3,
            evm_address="0x3456789012345678901234567890123456789012",
            collateral_wei=Decimal("1000"),
        )
        Miner.objects.create(
            hotkey="hotkey4",
            uid=4,
            evm_address="0x4567890123456789012345678901234567890123",
            collateral_wei=Decimal("2000"),
        )

        collateral = Collateral()
        result = collateral.list_miners_with_sufficient_collateral(min_amount_wei=1000)

        assert len(result) == 2
        hotkeys = {r.hotkey for r in result}
        assert hotkeys == {"hotkey3", "hotkey4"}

        for miner_data in result:
            assert miner_data.collateral_wei >= 1000


class TestSlashCollateral:
    @patch("compute_horde_validator.validator.collateral.default._get_private_key")
    @patch("compute_horde_validator.validator.collateral.default._get_collateral_abi")
    def test_slash_collateral_success(self, mock_get_abi, mock_get_private_key):
        """Test successful collateral slashing."""
        mock_get_private_key.return_value = "0x1234567890abcdef"
        mock_get_abi.return_value = [{"name": "slashCollateral", "type": "function"}]

        mock_w3 = Mock()
        mock_w3.to_checksum_address.return_value = "0x1234567890123456789012345678901234567890"
        mock_w3.eth.get_transaction_count.return_value = 1
        mock_w3.eth.gas_price = 20000000000
        mock_w3.eth.chain_id = 1
        mock_w3.eth.send_raw_transaction.return_value = HexBytes("0xabcdef1234567890")

        mock_receipt = {"status": 1, "transactionHash": HexBytes("0xabcdef1234567890")}
        mock_w3.eth.wait_for_transaction_receipt.return_value = mock_receipt

        mock_contract = Mock()
        mock_contract.functions.slashCollateral.return_value = Mock()
        mock_contract.events.Slashed.return_value.process_receipt.return_value = [
            {
                "event": "Slashed",
                "logIndex": 0,
                "transactionIndex": 0,
                "transactionHash": HexBytes("0xabcdef1234567890"),
                "address": "0x1234567890123456789012345678901234567890",
                "blockHash": HexBytes(
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                ),
                "blockNumber": 12345,
            }
        ]
        mock_w3.eth.contract.return_value = mock_contract

        mock_account = Mock()
        mock_account.address = "0x1234567890123456789012345678901234567890"
        mock_account.key = "0x1234567890abcdef"

        with patch(
            "compute_horde_validator.validator.collateral.default.Account.from_key",
            return_value=mock_account,
        ):
            collateral = Collateral()
            result = collateral.slash_collateral(
                w3=mock_w3,
                contract_address="0x1234567890123456789012345678901234567890",
                miner_address="0xabcdef1234567890abcdef1234567890abcdef12",
                amount_wei=1000,
                url="https://example.com",
            )

            assert isinstance(result, SlashedEvent)
            assert result.event == "Slashed"
            assert result.blockNumber == 12345
            assert result.transactionHash == HexBytes("0xabcdef1234567890")


class TestGetMinerCollateral:
    @patch("compute_horde_validator.validator.collateral.tasks.collateral")
    def test_get_miner_collateral(self, mock_collateral):
        """Test successful retrieval of miner collateral."""
        mock_w3 = Mock()
        mock_w3.to_checksum_address.return_value = "0x1234567890123456789012345678901234567890"

        mock_contract = Mock()
        mock_contract.functions.collaterals.return_value.call.return_value = 5000
        mock_w3.eth.contract.return_value = mock_contract

        mock_collateral.return_value._get_collateral_abi.return_value = [
            {"name": "collaterals", "type": "function"}
        ]

        result = get_miner_collateral(
            w3=mock_w3,
            contract_address="0x1234567890123456789012345678901234567890",
            miner_address="0xabcdef1234567890abcdef1234567890abcdef12",
            block_identifier=12345,
        )

        assert result == 5000
        assert mock_w3.to_checksum_address.call_count == 2
        mock_contract.functions.collaterals.return_value.call.assert_called_with(
            block_identifier=12345
        )


class TestGetEvmKeyAssociations:
    @pytest.mark.asyncio
    async def test_get_evm_key_associations(self):
        """Test successful retrieval of EVM key associations."""
        mock_subtensor = Mock()
        mock_associations = [
            ((1, 1), ("0x1234567890123456789012345678901234567890", "block1")),
            ((1, 2), ("0x2345678901234567890123456789012345678901", "block2")),
        ]

        async def mock_fetch(netuid, block_hash=None):
            return mock_associations

        mock_subtensor.subtensor_module.AssociatedEvmAddress.fetch = mock_fetch

        result = await get_evm_key_associations(
            subtensor=mock_subtensor, netuid=1, block_hash="block_hash"
        )

        expected = {
            1: "0x1234567890123456789012345678901234567890",
            2: "0x2345678901234567890123456789012345678901",
        }
        assert result == expected


class TestSyncCollaterals:
    @patch("compute_horde_validator.validator.collateral.tasks.get_web3_connection")
    @patch("compute_horde_validator.validator.collateral.tasks.collateral")
    @patch("compute_horde_validator.validator.collateral.tasks.get_evm_key_associations")
    @patch("compute_horde_validator.validator.collateral.tasks.get_miner_collateral")
    @patch("compute_horde_validator.validator.collateral.tasks._get_metagraph_for_sync")
    @patch("compute_horde_validator.validator.tasks.ShieldedBittensor")
    @pytest.mark.django_db(databases=["default", "default_alias"])
    def test_sync_collaterals_success(
        self,
        mock_shielded_bittensor,
        mock_get_metagraph,
        mock_get_collateral,
        mock_get_associations,
        mock_collateral,
        mock_get_web3,
        db,
    ):
        mock_bittensor_instance = Mock()
        mock_shielded_bittensor.return_value.__aenter__.return_value = mock_bittensor_instance
        mock_shielded_bittensor.return_value.__aexit__.return_value = None

        mock_neurons = [
            Mock(hotkey="hotkey1"),
            Mock(hotkey="hotkey2"),
        ]
        mock_block = Mock()
        mock_block.number = 12345
        mock_block.hash = "block_hash"

        mock_get_metagraph.return_value = (mock_neurons, Mock(), mock_block)
        mock_get_associations.return_value = {
            1: "0x1234567890123456789012345678901234567890",
            2: "0x2345678901234567890123456789012345678901",
        }

        mock_w3 = Mock()
        mock_get_web3.return_value = mock_w3

        mock_collateral.return_value.get_collateral_contract_address.return_value = "0xcontract"
        mock_get_collateral.return_value = 5000

        miner1 = Miner.objects.create(
            hotkey="hotkey1", uid=1, evm_address=None, collateral_wei=Decimal("0")
        )
        miner2 = Miner.objects.create(
            hotkey="hotkey2", uid=2, evm_address=None, collateral_wei=Decimal("0")
        )

        sync_collaterals()

        miner1.refresh_from_db()
        miner2.refresh_from_db()

        assert miner1.evm_address == "0x1234567890123456789012345678901234567890"
        assert miner1.collateral_wei == Decimal("5000")
        assert miner2.evm_address == "0x2345678901234567890123456789012345678901"
        assert miner2.collateral_wei == Decimal("5000")

        mock_get_associations.assert_called_once()
        mock_get_collateral.assert_called()
        mock_collateral.return_value.get_collateral_contract_address.assert_called_once()
