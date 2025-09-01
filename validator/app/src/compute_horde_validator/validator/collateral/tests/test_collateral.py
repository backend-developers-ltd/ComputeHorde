from decimal import Decimal
from unittest.mock import Mock, patch

from hexbytes import HexBytes

from compute_horde_validator.validator.collateral.default import Collateral
from compute_horde_validator.validator.collateral.types import SlashedEvent
from compute_horde_validator.validator.models import Miner


class TestCollateral:
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

        # Verify collateral amounts
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
