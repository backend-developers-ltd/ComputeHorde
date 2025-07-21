# Scoring Module

This module provides a clean, modular approach to calculating validator weights with decoupled dancing support.

## Overview

The scoring module provides a comprehensive solution for calculating validator weights with:

1. **Clean separation of concerns** - Each component has a single responsibility
2. **Testability** - Each function can be tested independently
3. **Modularity** - Easy to extend and modify
4. **Decoupled dancing support** - Built-in support for split distributions and bonuses

## Structure

```
scoring/
├── __init__.py          # Module exports
├── engine.py            # Main scoring engine (interface + implementation)
├── calculations.py      # Core scoring calculations and functions
├── models.py           # Internal data models and database models
├── test_engine.py      # Tests
└── README.md           # This file
```

## Components

### ScoringEngine Interface

The abstract interface that defines the contract for scoring engines. It provides:

- `calculate_scores_for_cycles()` - Main method to calculate scores for cycles
- Easy mocking for testing
- Extensibility for different scoring implementations

### DefaultScoringEngine

The default implementation that orchestrates the entire scoring process. It:

- Calculates scores from organic and synthetic jobs
- Applies decoupled dancing bonuses
- Handles split distributions
- Manages the entire scoring workflow

### Calculations

Core scoring calculation functions:

- `calculate_organic_scores()` - Calculate scores from organic jobs
- `calculate_synthetic_scores()` - Calculate scores from synthetic jobs  
- `combine_scores()` - Combine organic and synthetic scores
- `score_batch()` - Score a single batch with all complex logic
- `score_batches()` - Score multiple batches with decoupled dancing
- `horde_score()` - Advanced horde scoring algorithm
- `get_manifest_multiplier()` - Calculate manifest bonuses
- `get_penalty_multiplier()` - Calculate penalties for non-peak cycles
- `apply_decoupled_dancing_weights()` - Apply decoupled dancing adjustments

### Models

Internal data structures and database models:

- `SplitInfo` - Internal representation of a miner's split distribution
- `SplitStorage` - Internal storage for split information
- `MinerSplit` - Database model for storing split information
- `MinerSplitDistribution` - Database model for storing distribution percentages

## Database Models

The scoring module includes its own database models for split management:

### MinerSplit
Stores split information for miners in a group (same coldkey):
- `coldkey` - The miner's coldkey
- `cycle_start` - Start block of the cycle
- `cycle_end` - End block of the cycle  
- `validator_hotkey` - Validator hotkey
- `created_at` - Creation timestamp

### MinerSplitDistribution
Stores the distribution percentages for each hotkey in a split:
- `split` - Foreign key to MinerSplit
- `hotkey` - The miner's hotkey
- `percentage` - Distribution percentage (0.0000 to 1.0000)

## Usage

### Basic Usage

```python
from .scoring.engine import DefaultScoringEngine

engine = DefaultScoringEngine()
scores = await engine.calculate_scores_for_cycles(
    current_cycle_start=1000,
    previous_cycle_start=278,
    validator_hotkey="validator_hotkey"
)
```

### Batch Scoring

```python
from .scoring import score_batches

scores = score_batches(batches, validator_hotkey)
```

### Celery Task

The module integrates with Celery through the `calculate_and_set_weights` task:

```python
from .tasks import calculate_and_set_weights

# This will be called by Celery
calculate_and_set_weights.delay()
```

## Configuration

The module uses the following settings:

- `DYNAMIC_DANCING_BONUS` - Bonus multiplier for split changes (default: 0.1)
- `HORDE_SCORE_AVG_PARAM` - Alpha parameter for horde scoring
- `HORDE_SCORE_SIZE_PARAM` - Beta parameter for horde scoring
- `HORDE_SCORE_CENTRAL_SIZE_PARAM` - Delta parameter for horde scoring

## Testing

Run the tests with:

```bash
python manage.py test validator.scoring.test_engine
```

## Future Enhancements

- Support for different scoring algorithms
- Plugin architecture for custom scoring strategies
- Performance optimizations for large datasets
- Integration with paragons (when implemented)

## Decoupled Dancing Design

### Miner Split Exposure

Miners expose their split distributions through the `get_split_distribution()` method in their executor manager:

```python
async def get_split_distribution(self) -> dict[str, float] | None:
    """
    Get the split distribution for decoupled dancing.
    Returns the same split distribution to all validators.
    
    Returns:
        Dictionary mapping hotkeys to percentages, or None if no split
    """
    return {
        "hotkey1": 0.6,
        "hotkey2": 0.4
    }
```

**Important**: Miners expose the **same split distribution to all validators** for consistency. The split represents how the miner's coldkey distributes weights across its hotkeys.

### Validator Split Storage

Validators store splits per cycle and validator combination to track changes:

- `MinerSplit` - Stores split metadata (coldkey, cycle, validator)
- `MinerSplitDistribution` - Stores per-hotkey percentages

### Scoring Logic

1. **Base scores** - Calculate organic and synthetic job scores
2. **Split application** - Apply split distributions to group scores by coldkey
3. **Dancing bonus** - Apply bonus when split distributions change between cycles
4. **Final scores** - Return final scores with all adjustments applied

## Database Migration

**Note**: The `MinerSplit` and `MinerSplitDistribution` models need to be migrated to the database. Create a migration with:

```bash
python manage.py makemigrations validator --name add_miner_split_models
python manage.py migrate
```

## Available Functions

The scoring module provides the following functions:

### Core Calculations (`calculations.py`)
- `normalize()` - Normalize scores
- `sigmoid()` / `reversed_sigmoid()` - Sigmoid functions
- `horde_score()` - Advanced horde scoring
- `score_synthetic_jobs()` - Score synthetic jobs
- `score_organic_jobs()` - Score organic jobs
- `score_batch()` - Score a single batch
- `score_batches()` - Score multiple batches with dancing
- `get_executor_counts()` - Get executor counts
- `get_base_synthetic_score()` - Calculate base scores
- `get_manifest_multiplier()` - Calculate manifest bonuses
- `get_penalty_multiplier()` - Calculate penalties
- `apply_decoupled_dancing_weights()` - Apply dancing adjustments
- `get_coldkey_to_hotkey_mapping()` - Miner mapping
- `split_changed_from_previous_cycle()` - Check split changes 