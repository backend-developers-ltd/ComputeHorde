# Migration Guide

## Migrate Validator

To migrate your validator to a new server, you have to start your validator in the new server in a "migrating state".
In this state, the validator will not serve synthetic jobs and will not set scores or weights (so your old server keeps working).

Steps:

1. Create a new Ubuntu Server and execute the following command from your local machine (where you have your wallet files):

   ```shell
   curl -sSfL https://github.com/backend-developers-ltd/ComputeHorde/raw/master/install_validator.sh | MIGRATING=1 bash -s - SSH_DESTINATION HOTKEY_PATH
   ```
   
   Replace `SSH_DESTINATION` with your new server's connection info (i.e. `username@1.2.3.4`)
   and `HOTKEY_PATH` with the path of your hotkey (i.e. `~/.bittensor/wallets/my-wallet/hotkeys/my-hotkey`).
   This script installs necessary tools in the server, copies the keys and starts the validator in a migrating state.
   **NOTE:** the new server must be deployed with the same hotkey for the migration to be successful.
2. Login to your new server and start ssh local port forwarding to your old server's `HTTP_PORT` (in `.env` file or default 80).
   TODO: needs further explanation, or a mini-tutorial? :P
3. In your new validator admin panel, go to the "Constance" > "Config" and fill in the "Forwarded Port To Old Validator".
4. Wait for data to be synced?
   TODO: how long? 2h?
5. In your old validator admin panel, go to the "Constance" > "Config", un-tick "Serving" and save.
6. In your new validator admin panel, go to the "Constance" > "Config", tick "Serving", un-tick "Migrating" and save.
7. Check logs and wait for your new server to start sending jobs and setting weights.
8. Stop old server.


## Migrate Miner

To migrate your miner to a new server, you have to start your miner in the new server in a "migrating state".
In this state, the miner will not serve axon info (so your old server keeps working).

Steps:

1. Create a new Ubuntu Server and execute the following command from your local machine (where you have your wallet files):

   ```shell
   curl -sSfL https://github.com/backend-developers-ltd/ComputeHorde/raw/master/install_miner.sh | MIGRATING=1 bash -s - SSH_DESTINATION HOTKEY_PATH
   ```
   
   Replace `SSH_DESTINATION` with your new server's connection info (i.e. `username@1.2.3.4`)
   and `HOTKEY_PATH` with the path of your hotkey (i.e. `~/.bittensor/wallets/my-wallet/hotkeys/my-hotkey`).
   This script installs necessary tools in the server, copies the keys and starts the miner in a migrating state.
   **NOTE:** the new server must be deployed with the same hotkey for the migration to be successful.
2. In your new miner admin panel, go to the "CONSTANCE" > "Config", fill out `OLD_MINER_IP` and `OLD_MINER_PORT` and save. 
3. Wait for any running jobs to finish. To check job statuses, go to the "MINER" > "Accepted jobs" in your admin panel.
4. When there are no jobs running:
   1. In your old miner admin panel, go to the "CONSTANCE" > "Config", un-tick `SERVING` and save.
   2. In your new miner admin panel, go to the "CONSTANCE" > "Config", tick `SERVING` and save.
5. Check logs and admin panel to verify that:
   1. your old server is NOT receiving new jobs
   2. your new server have updated axon info with new ip/port
   3. your new server is receiving jobs
6. Compare the receipts in both admin panel to see all receipts have been migrated.
   After migration is complete, go to the "CONSTANCE" > "Config" in your new miner admin panel, un-tick `MIGRATING` and save.
7. Stop old server.
