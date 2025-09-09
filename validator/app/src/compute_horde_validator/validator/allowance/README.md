# Allowance Module

## Overview

This module provides facilities for:

1. calculating allowance (for all validators on the subnet)
2. includes allowance expiration
3. includes allowance block invalidation due to manifest drops
4. temporary allowance reservation - for pending jobs
5. finding a list of miners suitable for a job (all of them):
   1. ones that have at least the required amount of allowance left
   2. sorted by:
      1. earliest unspent/unreserved block
      2. highest total allowance percentage left
6. validating jobs from other validators (in terms of allowance)
7. Each block yields a contribution to each validator-miner-executorClass triplet allowance equal to: 
   "block_length * validator_stake_share * executor_count". the length of the N-th block is defined as 
   "block_creation_timestamp(N+1) - block_creation_timestamp(N)". Therefore a block yields allowance contribution only 
   after the next one is minted.
8. this module does not facilitate locking, i.e. between reading the available allowance and reserving it the state 
   might change - it is the responsibility of modules dependent on this one to ensure locking/transactions (as there 
   might be other factors to include).

As of now, this module will likely not perform well if the validator hotkey or bittensor chain is changed with the 
database state left intact.

To properly function, this module needs to have a lite subtensor and an archive subtensor address configured. Archive
is optional, but will result in an inability to properly backfill all the required information after a longer downtime
or upon initial startup. That is useful for testing on private chains.

## Intended Flow

In order to properly use this module, when sending a job to a miner adhere to the following pattern:

1. find miners with enough allowance
2. reserve one such miner
3. Send the initial job request to the miner:
   1. if accepts, proceed
   2. if it declines or times out: undo the reservation and chose another one
4. if the job is succsesfull: spend the reservation
5. otherwise undo it - reservaitons have time outs, so if this fails the allowance won't be lost


## Overrides/Debug Mode

1. allowance checking can be switched off entirely in dynamic params
2. this module does not enforce allowance checking, so if the `routing` module has an override (for example in local 
   development mode) and just sends jobs then they might be sent in violation of allowance

## Public Interface

### Models
None

### Methods

1. find_miners_with_allowance
2. reserve allowance (with an expiry time)
3. undo allowance reservation
4. spend allowance
5. validate foreign receipt
6. return block number
7. return manifests
8. return the metagraph (include ddos-shield-mangled addresses)

Access them using the singleton `allowance.allowance()`.

## Dependencies

1. core (system events and stuff)