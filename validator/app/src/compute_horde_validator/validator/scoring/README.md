# Scoring Module

This module provides an interface for calculating scores across cycles and applying dancing bonuses based on split distributions.

## Overview

The scoring module calculates scores for miners based on their performance in organic and synthetic jobs. It groups miners by their coldkey and applies bonuses when miner change their split distributions between cycles.

## Basic Usage

```python
from compute_horde_validator.validator.scoring import create_scoring_engine

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
- **Scoring**: Each completed job contributes a configurable score (default: 1.0)
- **Filtering**: Only counts jobs that are:
  - Completed successfully
  - Not cheated
  - Not from trusted miners
- **Executor Classes**: Different executor classes have different weights

### Synthetic Jobs
- **Source**: Validator-generated test jobs
- **Scoring**: Each completed job contributes its individual score
- **Filtering**: Only counts jobs that are completed successfully
- **Executor Classes**: Different executor classes have different weights

## Score Calculation Process

The scoring process follows these steps:

### 1. Job Collection and Filtering

The system first collects all jobs from the current cycle and applies filtering rules:

**Organic Jobs Filtering:**
- Only includes jobs with status "COMPLETED"
- Excludes jobs marked as cheated
- Excludes jobs from trusted miners
- Only processes jobs with configured executor classes

**Synthetic Jobs Filtering:**
- Only includes jobs with status "COMPLETED"
- Excludes excused jobs (unless properly excused with sufficient validator stake)

### 2. Raw Score Calculation

For each type of job, the system calculates raw scores:

**Organic Jobs:** Each completed job contributes a fixed score (default: 1.0 per job). The system sums up all valid organic jobs for each miner.

**Synthetic Jobs:** Each completed job contributes its individual score. The system sums up all valid synthetic jobs for each miner.

### 3. Executor Class Grouping

Jobs are grouped by their executor class (e.g., `spin_up-4min.gpu-24gb`, `always_on.llm.a6000`). This grouping allows the system to apply different weights and normalization rules to different types of computational resources.

### 4. Score Combination

For each executor class, the system combines organic and synthetic scores. This gives a total raw score per miner for each executor class, representing their performance across both real user jobs and validator test jobs.

### 5. Split Distribution

The system queries each miner's current split distribution to understand how they allocate their computational resources across multiple hotkeys. Scores are then distributed according to these splits. For example, if a miner has an 80/20 split between two hotkeys, 80% of their score goes to the first hotkey and 20% to the second.

### 6. Dancing Bonus

The system compares each miner's current split distribution with their previous cycle's distribution. If the distribution has changed (indicating the miner has "danced" by reallocating resources), a bonus multiplier is applied to the main hotkey in that group.

### 7. Normalization and Weight Application

For each executor class, the system normalizes scores across the entire batch of miners in that class.

### 8. Final Score Accumulation

The system accumulates scores from all executor classes for each hotkey. A single hotkey may receive scores from multiple executor classes, and these are summed together to produce the final score for that hotkey.

## Executor Classes and Weights

Executor classes represent different types of computational resources and have configurable weights:

### Configuration
```python
DYNAMIC_EXECUTOR_CLASS_WEIGHTS = {
    "spin_up-4min.gpu-24gb": 99,    # GPU instances with 24GB memory
    "always_on.llm.a6000": 1        # Always-on LLM instances
}
```

### Weight Application
- Weights control the relative importance of different executor classes
- Higher weights result in higher scores for that executor class
- Weights are applied during normalization
- Unconfigured executor classes are excluded from scoring

### Example Weight Impact
```python
# With weights: spin_up-4min.gpu-24gb=99, always_on.llm.a6000=1
# Same raw scores but different final scores:
# GPU jobs: (5.0 / 5.0) * 99 = 99.0
# LLM jobs: (5.0 / 5.0) * 1 = 1.0
```

## Normalization Process

### Per-Executor-Class Batch Normalization
The system normalizes scores across the entire executor class batch:

```python
# Total score for entire executor class
total_executor_score = sum(all_hotkey_scores_in_executor_class)

# Normalize each hotkey's score
normalized_score = (hotkey_score / total_executor_score) * weight
```

## Dancing Bonus System

The dancing bonus encourages miners to dynamically adjust their resource allocation across different hotkeys.

### What is Dancing?

"Dancing" refers to miners changing how they distribute their computational resources across multiple hotkeys. This is tracked through **split distributions**.

### Split Distribution

A split distribution defines how a miner divides their total score among their hotkeys:

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
4. **Bonus Application**: If distributions changed, applies a bonus multiplier (e.g., 1.1x) to the main hotkey in the group.

### Example Dancing Scenario

```python
# Previous cycle: 50/50 split
previous_splits = {"hotkey1": 0.5, "hotkey2": 0.5}

# Current cycle: 70/30 split (dancing occurred)
current_splits = {"hotkey1": 0.7, "hotkey2": 0.3}

# Result: bonus applied to the hotkey1
```

## Configuration

### Environment Variables

The scoring system is configured through Django settings:

```python
# Executor class weights
DYNAMIC_EXECUTOR_CLASS_WEIGHTS = "spin_up-4min.gpu-24gb=99,always_on.llm.a6000=1"

# Organic job scoring
DYNAMIC_ORGANIC_JOB_SCORE = 1.0  # Score per organic job
DYNAMIC_SCORE_ORGANIC_JOBS_LIMIT = -1  # -1 = unlimited

# Dancing bonus
DYNAMIC_DANCING_BONUS = 0.1  # 10% bonus for dancing

# Synthetic job scoring
DYNAMIC_EXCUSED_SYNTHETIC_JOB_SCORE = 1.0  # Score for excused jobs
```

## API Reference

### Creating a ScoringEngine

The scoring system provides a factory function to create the engine:

```python
from compute_horde_validator.validator.scoring import create_scoring_engine

# Create the default scoring engine
engine = create_scoring_engine()
```

### ScoringEngine Interface

```python
class ScoringEngine:
    async def calculate_scores_for_cycles(
        self, 
        current_cycle_start: int, 
        previous_cycle_start: int, 
        validator_hotkey: str
    ) -> dict[str, float]:
        """
        Calculate scores for two cycles and apply dancing.
        
        Args:
            current_cycle_start: Start block of current cycle
            previous_cycle_start: Start block of previous cycle
            validator_hotkey: Validator hotkey for split retrieval
            
        Returns:
            Dictionary mapping hotkey to final score
        """
```
