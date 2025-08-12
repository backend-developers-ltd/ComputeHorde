# Routing Module

## Overview

This module is responsible for selecting the most suitable miner to execute a given organic job. The selection process is based on a variety of criteria to ensure that jobs are routed to reliable and capable miners who can fulfill the job's requirements.

The key responsibilities of this module include:
1.  **Miner Filtering**: Identifying active and eligible miners based on executor class, manifest freshness, and blacklist status.
2.  **Resource and Capacity Checks**: Verifying that a miner has sufficient compute time allowance, available executors, and meets the minimum collateral requirements.
3.  **Time-based Validation**: Ensuring there is enough time left in the current blockchain cycle to complete the job.
4.  **Optimal Miner Selection**: Sorting the eligible miners to pick the best candidate, prioritizing those with a higher percentage of remaining allowance, and using collateral as a tie-breaker.
5.  **Preliminary Reservation**: Creating a short-lived reservation for the selected miner to prevent race conditions where multiple validator instances might select the same miner for different jobs.
6.  **Special Routing Modes**: Providing overrides for debugging and routing to trusted miners.

## Intended Flow

When a request to execute an organic job is received, the routing module performs the following steps:

1.  **Job Request Reception**: The process starts with an `OrganicJobRequest` (currently, this is a `V2JobRequest`).
2.  **Miner Selection**: The `pick_miner_for_job_request` function automates the selection by:
    1.  Filtering for miners that have published a manifest for the required `executor_class` within the last 4 hours and are not blacklisted.
    2.  Filtering out miners who do not meet the `DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI` requirement.
    3.  Filtering out miners who do not have enough `remaining_allowance` for the job (this check can be disabled via dynamic config).
    4.  Verifying that there is sufficient time remaining in the current bittensor cycle for the job to complete (this check can be disabled via dynamic config).
    5.  Sorting the remaining miners, preferring those with the highest percentage of remaining allowance. The total collateral serves as a tie-breaker.
3.  **Executor Availability Check**: For each miner in the sorted list, it checks if there is an available executor slot by comparing the number of online executors with the number of ongoing jobs.
4.  **Preliminary Reservation**: Once a miner with an available executor is found, a `MinerPreliminaryReservation` is created for the job. This reservation is temporary and prevents the same miner from being picked immediately for another job.
5.  **Miner Assignment**: The function returns the selected `Miner` object. The calling service is then responsible for sending the job to this miner.

## Overrides and Debug Modes

The standard routing logic can be bypassed using the following mechanisms:

-   **`DEBUG_MINER_KEY`**: If this setting is defined, all jobs are routed directly to the specified miner, skipping all checks.
-   **Trusted Miner Mode**: If a `V2JobRequest` has the `on_trusted_miner=True` flag, the job is routed to a designated trusted miner.
-   **Dynamic Configuration**: Certain checks can be toggled at runtime:
    -   `DYNAMIC_CHECK_ALLOWANCE_WHILE_ROUTING`: Set to `False` to disable checking for sufficient allowance.
    -   `DYNAMIC_ALLOW_CROSS_CYCLE_ORGANIC_JOBS`: Set to `True` to disable the check for sufficient time remaining in the cycle.

## Public Interface

### Abstract Base Class
-   `RoutingBase`: Defines the abstract interface for any routing implementation.

### Methods
-   `async def pick_miner_for_job_request(request: OrganicJobRequest) -> Miner`:
    -   The main entry point for selecting a miner for a job.
    -   On success, it returns the selected `Miner` instance.
    -   On failure, it raises one of the exceptions listed below.

## Configuration Parameters

The behavior of the routing module is controlled by several dynamic configuration parameters:

-   `DYNAMIC_CHECK_ALLOWANCE_WHILE_ROUTING`: A boolean that enables or disables the compute time allowance check during routing.
-   `DYNAMIC_ALLOW_CROSS_CYCLE_ORGANIC_JOBS`: A boolean that allows jobs to be scheduled even if they are expected to complete in the next cycle.
-   `DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI`: The minimum amount of collateral a miner must have to be considered for jobs.
-   `DYNAMIC_ROUTING_PRELIMINARY_RESERVATION_TIME_SECONDS`: The duration (in seconds) for which a preliminary reservation is valid.
-   `DYNAMIC_ORGANIC_JOB_ALLOWED_LEEWAY_TIME`: A buffer time (in seconds) added to the job's execution time when checking if it can be completed within the current cycle.
-   `DYNAMIC_EXECUTOR_RESERVATION_TIME_LIMIT`: The time limit for an executor reservation.
-   `DYNAMIC_EXECUTOR_STARTUP_TIME_LIMIT`: The time limit for an executor to start up.
