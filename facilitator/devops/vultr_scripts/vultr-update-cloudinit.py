#!/usr/bin/env python
# update cloud-init data
# this should be used only to UPDATE the data, initialization should be done via Terraform
# see vultr_tf/core/vultr-cloud-init.tftpl
import subprocess
from pathlib import Path

pwd = Path(__file__).parent

# cloud-init script
# use `vultr-cli instance user-data get <instanceID>` to get existing data
user_data = pwd / "userdata.txt"
assert user_data.exists()

with open(pwd / "instances_id.txt") as f:
    for instance_id in f.readlines():
        print("instance id", instance_id)
        # res = subprocess.check_output(['vultr-cli', 'instance', 'user-data', 'get', instance_id.strip()])
        res = subprocess.check_output(
            [
                "vultr-cli",
                "instance",
                "user-data",
                "set",
                instance_id.strip(),
                "-d",
                str(user_data),
            ]
        )
        print("res", res, type(res))
