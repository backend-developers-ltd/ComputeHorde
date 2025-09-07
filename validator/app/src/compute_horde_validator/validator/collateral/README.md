# Collateral Module

This module manages miner collateral operations including querying collateral amounts, slashing collateral, and synchronizing collateral data from the blockchain.

## API

Default implementation lives in `compute_horde_validator.validator.collateral.default.Collateral` and implements the abstract interface in `compute_horde_validator.validator.collateral.base.CollateralBase`.

### Core Interface

```python
# List miners with sufficient collateral
miners: list[MinerCollateral] = collateral().list_miners_with_sufficient_collateral(
    min_amount_wei: int
)
"""
Returns miners whose collateral is at least min_amount_wei.
Each MinerCollateral contains:
- hotkey: str - Miner's hotkey
- collateral_wei: int - Collateral amount in Wei
"""

# Slash collateral from a miner
await collateral().slash_collateral(
    miner_hotkey: str,
    url: str,
)
"""
Slash collateral from a miner.
- miner_hotkey: SS58 address of the miner to slash (will be mapped to EVM address internally)
- url: URL containing information about the slash (will be validated and checksummed)

The slashing amount is determined by the DYNAMIC_COLLATERAL_SLASH_AMOUNT_WEI configuration.

Returns None.
Raises SlashCollateralError if transaction fails, slash amount is invalid, or miner not found.
"""

# Get collateral contract address (async)
contract_address: str | None = await collateral().get_collateral_contract_address()
"""
Returns the current collateral contract address or None if unavailable.
Address is fetched from the validator's commitment on the Bittensor network.
"""
```

### Utility Functions

```python
# Query collateral amount for a specific miner
w3 = Web3(...)
collateral_amount: int = get_miner_collateral(
    w3: Web3,
    contract_address: str,
    miner_address: str,
    block_identifier: int | None = None,
)
"""
Query the collateral amount for a given miner address.
- w3: Web3 instance for blockchain interaction
- contract_address: Address of the Collateral contract
- miner_address: EVM address of the miner to query
- block_identifier: Block number to query (None for latest block)

Returns collateral amount in Wei.
"""
```

### Data Models

```python
# MinerCollateral - Information about a miner's collateral
class MinerCollateral(BaseModel):
    hotkey: str                    # Miner's hotkey
    collateral_wei: int           # Collateral amount in Wei

```

## Background Tasks

### sync_collaterals

**Celery Task**: `sync_collaterals`

Synchronizes miner EVM addresses and collateral amounts from the blockchain.

**Process**:
1. Fetches current metagraph data from Bittensor
2. Retrieves EVM key associations for all miners
3. Updates miner EVM addresses in the database
4. Queries collateral amounts from the smart contract
5. Updates collateral amounts in the database

## Configuration

- **Settings**:
  - `BITTENSOR_WALLET_DIRECTORY`: Directory containing wallet files
  - `BITTENSOR_NETWORK`: Bittensor network to connect to
  - `BITTENSOR_NETUID`: Subnet UID for the compute horde
  - `DEFAULT_DB_ALIAS`: Database alias for system events

- **Required Files**:
  - `collateral_abi.json`: Smart contract ABI for collateral operations
  - Wallet files with EVM private keys for slashing operations

## Error Types

- `CollateralException`: Base exception for collateral operations
- `SlashCollateralError`: Transaction failure during slashing
