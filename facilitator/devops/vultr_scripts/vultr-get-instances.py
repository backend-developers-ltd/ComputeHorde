#!/usr/bin/env python
# get list of all instances in Vultr account
# save their IDs and IPs into files which will be used by \
# `vultr-deploy.py` and `vultr-update-cloudinit.py`

import subprocess
from pathlib import Path

pwd = Path(__file__).parent

instance_id = pwd / "instances_id.txt"
instance_ip = pwd / "instances_ip.txt"

res = subprocess.check_output(["vultr-cli", "instance", "list", "ipv4"]).decode("utf-8").split("\n")

ids = []
ips = []
for line in res[1:]:  # skip header
    line_items = line.split("\t")
    if len(line_items) != 13:
        continue
    ids.append(line_items[0].strip())
    ips.append(line_items[1].strip())

with open(instance_ip, "w") as f:
    f.write("\n".join(ips))

with open(instance_id, "w") as f:
    f.write("\n".join(ids))
