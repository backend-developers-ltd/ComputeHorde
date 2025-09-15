# Routing Module

## Overview

This module provides facilities for selecting a suitable miner for a given job request. Its primary responsibility is to translate a job's requirements into a concrete miner that can execute it, while respecting the constraints imposed by the `allowance` module.

The key functionalities are:
1. Finding a list of miners that have sufficient allowance for a job.
2. Reserving the allowance for the selected miner to ensure it's not used by another concurrent job.
3. Providing a structured `JobRoute` object containing the selected miner's details and the allowance reservation identifier.
4. Supporting debug/override modes for local development and trusted miners.
5. Filtering miners by on-chain collateral before attempting reservation. This is optional, controlled through a dynamic config.

This module works in close conjunction with the `allowance` module, which is responsible for all allowance-related calculations and state management.

## Intended Flow

When a new job request arrives, the following sequence of operations should be performed:

1. Call `routing().pick_miner_for_job_request(job_request)` to get a `JobRoute`.
2. This function will internally:
    1. Query the `allowance` module to get a list of miners with enough allowance for the job's required executor-seconds.
    2. Apply a collateral threshold filter if the dynamic config value `DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI` is > 0. Only miners whose recorded collateral is at least that many Wei are kept. If all miners are filtered out a `NotEnoughCollateralException` is raised (a warning is logged indicating how many miners were filtered).
    3. Sort the remaining suitable miners and iterate through them, attempting to reserve the allowance for one of them.
        - Sorting logic: miners are ranked primarily by a per-executor reliability score (derived from recent recorded incidents for the miner and normalized by the executor count for the requested class). Higher scores are preferred. For miners with the same reliability score, the original order returned by the allowance module is kept.
        - Availability check: before trying to reserve allowance the router checks current ongoing jobs (via the `receipts` subsystem) and skips miners whose ongoing jobs are equal to or exceed their known executor count for the requested class.
    4. The first successful reservation will result in a `JobRoute` being returned.
3. If a `JobRoute` is successfully obtained, it can be used to execute the job on the specified miner. The `allowance_reservation_id` from the route must be stored and used later to either spend or release the reservation based on the job's outcome. `allowance_blocks` from the route needs to be used for the block ids in the generated receipts.
4. If `pick_miner_for_job_request` raises an exception (`NotEnoughAllowanceException` or `AllMinersBusy`), the job cannot be processed at this time and should be rejected.

## Overrides/Debug Mode

The routing module provides several overrides to facilitate development and special job handling:

1. **DEBUG_MINER_KEY**: If the `settings.DEBUG_MINER_KEY` is set, all jobs will be routed to this specific miner without any allowance checks. The returned `JobRoute` will have `allowance_reservation_id=None`.
2. **TRUSTED_MINER_FAKE_KEY**: If a job request has the `on_trusted_miner` flag set to `True`, the job is routed to a pseudo-miner identified by `TRUSTED_MINER_FAKE_KEY`. This also bypasses allowance checks, and the `JobRoute` will have `allowance_reservation_id=None` and `allowance_blocks=None`.

## Public Interface

### Models

The primary data structure returned by this module is `JobRoute`.

- `JobRoute`: A dataclass containing:
    - `miner`: An `allowance.types.Miner` object with the miner's connection details (address, port, hotkey).
    - `allowance_blocks`: An optional list of integers representing the allowance reserved block IDs. This is `None` for trusted miner routes.
    - `allowance_reservation_id`: An optional integer representing the ID of the allowance reservation. This is `None` for debug/trusted miner routes.

### Methods

The module exposes a singleton `routing()` which provides the following method:

1. `async def pick_miner_for_job_request(self, request: OrganicJobRequest) -> JobRoute`:
    - The main entry point for the module.
    - Takes an `OrganicJobRequest` as input.
    - Returns a `JobRoute` for a suitable miner.
    - Raises:
        - `NotEnoughAllowanceException`: If no miners have enough allowance for the job.
        - `AllMinersBusy`: If all suitable miners with enough allowance could not be reserved (e.g., due to concurrent reservation attempts).

2. `async def report_miner_incident(self, type: MinerIncidentType, hotkey_ss58address: str, job_uuid: str, executor_class: ExecutorClass) -> None`:
    - Records an incident for a miner (for example: job rejection, failure, or other executor-level problems).
    - Parameters:
        - `type`: the incident type (see `MinerIncidentType`).
        - `hotkey_ss58address`: the miner's hotkey address used to identify the miner.
        - `job_uuid`: the job identifier related to the incident.
        - `executor_class`: the executor class under which the incident occurred.
    - Effect: incidents are persisted (via the `MinerIncident` model) and are used when computing miners' reliability scores that influence future routing decisions.

Access the routing functionality using the singleton: `routing.routing()`.

## Dependencies

This module has a strong dependency on:

1. **`allowance` module**: For finding miners with sufficient allowance and for reserving/managing that allowance.
