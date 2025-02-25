#!/usr/bin/env python
# deploy to list of IPs from `instances_ip.txt` (see `vultr-get-instances.py`)

import subprocess
from pathlib import Path

pwd = Path(__file__).parent


with open(pwd / "instances_ip.txt") as f:
    ips = f.readlines()

errs = []
for ip in ips:
    print("deploying to", ip)
    try:
        res = subprocess.Popen(
            ["git", "push", f"root@{ip.strip()}:~/repos/project-central.git"],
            env={
                "GIT_SSH_COMMAND": "ssh -o ConnectTimeout=10  -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
            },
        ).communicate()
    except subprocess.CalledProcessError:
        errs.append(ip)
    else:
        print("res", res)

for err_ip in errs:
    print("error deploying to", err_ip)
