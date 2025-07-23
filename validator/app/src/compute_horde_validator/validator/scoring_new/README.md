# Scoring Module

This module provides a clean interface for calculating validator scores across cycles and applying dancing bonuses based on split distributions.

## Overview

The scoring module calculates scores for validators based on their performance in organic and synthetic jobs. It groups validators by their coldkey (miner identity) and applies bonuses when validators change their split distributions between cycles.

## Basic Usage

```python
from compute_horde_validator.validator.scoring_new import create_scoring_engine

# Create scoring engine
engine = create_scoring_engine()

# Calculate scores for current and previous cycles
scores = await engine.calculate_scores_for_cycles(
    current_cycle_start=1000,
    previous_cycle_start=278,
    validator_hotkey="your_validator_hotkey"
)

# scores is a dictionary mapping hotkey to final score
# Example: {"hotkey1": 15.5, "hotkey2": 8.2, "hotkey3": 12.1}
```

## How Scoring Works

The scoring system evaluates validator performance across two types of jobs:

### Organic Jobs
- **Source**: Real user-submitted jobs
- **Scoring**: Each completed job contributes a configurable score
- **Filtering**: Only counts jobs that are:
  - Completed successfully
  - Not cheated
  - Not from trusted miners
- **Executor Classes**: Different executor classes have different weights

### Synthetic Jobs  
- **Source**: System-generated test jobs
- **Scoring**: Based on executor class performance and weights
- **Purpose**: Ensures validators maintain infrastructure for different job types

### Score Calculation Process

1. **Job Collection**: Gathers all organic and synthetic jobs for the current cycle
2. **Executor Class Scoring**: Calculates scores per executor class with weighted contributions
3. **Hotkey Aggregation**: Sums scores per hotkey across all job types
4. **Coldkey Grouping**: Groups hotkeys by their coldkey (miner identity)
5. **Dancing Bonus**: Applies bonus if split distributions changed (see below)
6. **Final Scores**: Returns individual hotkey scores

## Dancing Bonus System

The dancing bonus encourages validators to dynamically adjust their resource allocation across different hotkeys.

### What is Dancing?

"Dancing" refers to miners changing how they distribute their computational resources across multiple hotkeys. This is tracked through **split distributions**.

### Split Distribution

A split distribution defines how a miner (coldkey) divides their total score among their hotkeys:

```python
# Example: Miner "miner1" has two hotkeys with 60/40 split
split_distribution = {
    "hotkey1": 0.6,  # 60% of total score
    "hotkey2": 0.4   # 40% of total score
}
```

### How Dancing Bonus Works

1. **Previous Cycle**: Records split distributions from the previous cycle
2. **Current Cycle**: Calculates new split distributions based on current performance
3. **Comparison**: Checks if split distributions changed between cycles
4. **Bonus Application**: If distributions changed, applies a bonus multiplier (e.g., 1.1x) to all hotkeys in that coldkey group

### Example Dancing Scenario

```python
# Previous cycle: 50/50 split
previous_splits = {"hotkey1": 0.5, "hotkey2": 0.5}

# Current cycle: 70/30 split (dancing occurred)
current_splits = {"hotkey1": 0.7, "hotkey2": 0.3}

# Result: 1.1x bonus applied to both hotkey1 and hotkey2 scores
```

### Benefits of Dancing

- **Resource Optimization**: Encourages miners to adapt to changing network demands
- **Load Balancing**: Promotes distribution of work across different hotkeys
- **Network Health**: Prevents stagnation in resource allocation patterns

## Interface

The scoring engine provides a single method:

```python
async def calculate_scores_for_cycles(
    self, 
    current_cycle_start: int,
    previous_cycle_start: int, 
    validator_hotkey: str
) -> Dict[str, float]:
    """
    Calculate scores for two cycles and apply dancing bonuses.
    
    Args:
        current_cycle_start: Start block of current cycle
        previous_cycle_start: Start block of previous cycle  
        validator_hotkey: Validator hotkey for split retrieval
        
    Returns:
        Dictionary mapping hotkey to final score
    """
```

## Configuration

The scoring system uses dynamic configuration for:
- **Organic job scores**: Points per completed organic job
- **Synthetic job weights**: Weights for different executor classes
- **Dancing bonus multiplier**: Bonus applied when splits change
- **Score limits**: Maximum scores per hotkey

These values are managed through the dynamic configuration system and can be updated without code changes. 