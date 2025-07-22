# Scoring Module

This module provides a clean interface for calculating scores across cycles and applying decoupled dancing bonuses.

## Architecture

The scoring module follows a clean architecture pattern with separate interface and implementation:

- **`interface.py`** - Contains the abstract `ScoringEngine` interface
- **`engine.py`** - Contains the concrete `DefaultScoringEngine` implementation
- **`factory.py`** - Contains the factory for creating engine instances
- **`calculations.py`** - Contains core scoring calculation functions (internal)
- **`models.py`** - Contains Django models for split distributions (internal)
- **`exceptions.py`** - Contains custom exceptions for error handling (internal)

## Basic Usage

```python
from compute_horde_validator.validator.scoring import create_scoring_engine, ScoringEngine

# Create scoring engine using factory
engine: ScoringEngine = create_scoring_engine()

# Calculate scores for multiple cycles
scores = await engine.calculate_scores_for_cycles(
    current_cycle_start=1000,
    previous_cycle_start=278,
    validator_hotkey="your_validator_hotkey"
)
```

## Factory Usage

```python
from compute_horde_validator.validator.scoring import create_scoring_engine, get_available_engine_types

# Get available engine types
available_types = get_available_engine_types()
print(f"Available engine types: {available_types}")  # ['default']

# Create specific engine type
engine = create_scoring_engine("default")

# Create default engine (same as above)
engine = create_scoring_engine()
```

## Interface Design

The `ScoringEngine` interface provides a single method:

```python
async def calculate_scores_for_cycles(
    self, 
    current_cycle_start: int,
    previous_cycle_start: int,
    validator_hotkey: str
) -> Dict[str, float]:
    """
    Calculate scores for two cycles and apply decoupled dancing.
    
    Args:
        current_cycle_start: Start block of current cycle
        previous_cycle_start: Start block of previous cycle
        validator_hotkey: Validator hotkey for split retrieval
        
    Returns:
        Dictionary mapping hotkey to final score
    """
```

## Decoupled Dancing Design

The decoupled dancing mechanism works as follows:

1. **Grouping**: Hotkeys are grouped by their coldkey (miner identity)
2. **Split Distribution**: Each coldkey group can have a split distribution that defines how the total score is divided among hotkeys
3. **Bonus Application**: If the split distribution changes between cycles, a bonus is applied to encourage dynamic behavior
4. **Individual Hotkeys**: Hotkeys without a coldkey or split distribution are processed individually

### Split Distribution Example

```python
# Coldkey "miner1" has two hotkeys with 60/40 split
split_distribution = {
    "hotkey1": 0.6,  # 60% of total score
    "hotkey2": 0.4   # 40% of total score
}
```

## Testing

For testing purposes, you can create mock implementations of the `ScoringEngine` interface:

```python
from unittest.mock import Mock
from compute_horde_validator.validator.scoring import ScoringEngine

class MockScoringEngine(ScoringEngine):
    def __init__(self, return_scores: dict[str, float]):
        self.return_scores = return_scores
    
    async def calculate_scores_for_cycles(
        self, 
        current_cycle_start: int,
        previous_cycle_start: int,
        validator_hotkey: str
    ) -> dict[str, float]:
        return self.return_scores

# Usage in tests
mock_engine = MockScoringEngine({"hotkey1": 10.0, "hotkey2": 5.0})
scores = await mock_engine.calculate_scores_for_cycles(1000, 278, "validator")
``` 