#!/bin/bash
set -euxo pipefail

# ch-test-miner-1-exec-*
./install_executor.sh root@46.224.2.93
./install_executor.sh root@46.224.6.24
./install_executor.sh root@23.88.120.218
./install_executor.sh root@188.245.59.231
./install_executor.sh root@168.119.61.199

# ch-test-miner-2-exec-*
./install_executor.sh root@46.224.10.35
./install_executor.sh root@46.224.10.60
./install_executor.sh root@91.99.180.251
./install_executor.sh root@91.98.41.123
./install_executor.sh root@46.224.9.240

# ch-test-miner-3-exec-*
./install_executor.sh root@91.99.199.133
./install_executor.sh root@159.69.93.27
./install_executor.sh root@46.224.10.58
./install_executor.sh root@46.62.216.87
./install_executor.sh root@46.62.226.183

# ch-test-miner-4-exec-*
./install_executor.sh root@65.21.61.176
./install_executor.sh root@46.62.213.29
./install_executor.sh root@95.216.147.118
./install_executor.sh root@46.62.159.73
./install_executor.sh root@65.108.94.35

# ch-test-miner-5-exec-*
./install_executor.sh root@91.98.199.228
./install_executor.sh root@46.224.21.172
./install_executor.sh root@91.98.17.17
./install_executor.sh root@91.99.133.118
./install_executor.sh root@159.69.13.112
