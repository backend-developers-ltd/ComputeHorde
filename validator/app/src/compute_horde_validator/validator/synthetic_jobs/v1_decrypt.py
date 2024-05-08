import csv
import hashlib
import json
import pickle
import re
import shutil
import subprocess
from base64 import b64encode

from cryptography.fernet import Fernet


def run_cmd(cmd):
    try:
        return subprocess.check_output(cmd, shell=True).decode()
    except Exception:
        return ""

def scrape_specs() -> dict[str, any]:
    data: dict[str, any] = {}

    gpus = []
    try:
        nvidia_cmd = run_cmd(
            "nvidia-smi --query-gpu=name,driver_version,name,memory.total,compute_cap,power.limit,clocks.gr,clocks.mem --format=csv"
        ).splitlines()
        csv_data = csv.reader(nvidia_cmd)
        header = [x.strip() for x in next(csv_data)]
        for row in csv_data:
            row = [x.strip() for x in row]
            gpu_data = dict(zip(header, row))
            gpus.append(
                {
                    "name": gpu_data["name"],
                    "driver": gpu_data["driver_version"],
                    "capacity": gpu_data["memory.total [MiB]"].split(" ")[0],
                    "cuda": gpu_data["compute_cap"],
                    "power_limit": gpu_data["power.limit [W]"].split(" ")[0],
                    "graphics_speed": gpu_data["clocks.current.graphics [MHz]"].split(
                        " "
                    )[0],
                    "memory_speed": gpu_data["clocks.current.memory [MHz]"].split(" ")[
                        0
                    ],
                }
            )
    except Exception as exc:
        # print(f'Error processing scraped gpu specs: {e}', flush=True)
        data["gpu_scrape_error"] = repr(exc)
    data["gpu"] = {"details": gpus, "count": len(gpus)}

    data["cpu"] = {}
    try:
        lscpu_output = run_cmd("lscpu")
        data["cpu"]["model"] = re.search(
            r"Model name:\s*(.*)$", lscpu_output, re.M
        ).group(1)
        data["cpu"]["count"] = int(
            re.search(r"CPU\(s\):\s*(.*)", lscpu_output).group(1)
        )

        cpu_data = run_cmd('lscpu --parse=MHZ | grep -Po "^[0-9,.]*$"').splitlines()
        data["cpu"]["clocks"] = [float(x) for x in cpu_data]
    except Exception as exc:
        # print(f'Error getting cpu specs: {e}', flush=True)
        data["cpu_scrape_error"] = repr(exc)

    data["ram"] = {}
    try:
        with open("/proc/meminfo") as f:
            meminfo = f.read()

        for name, key in [
            ("MemAvailable", "available"),
            ("MemFree", "free"),
            ("MemTotal", "total"),
        ]:
            data["ram"][key] = int(
                re.search(rf"^{name}:\s*(\d+)\s+kB$", meminfo, re.M).group(1)
            )
        data["ram"]["used"] = data["ram"]["total"] - data["ram"]["free"]
    except Exception as exc:
        # print(f"Error reading /proc/meminfo; Exc: {e}", file=sys.stderr)
        data["ram_scrape_error"] = repr(exc)

    data["hard_disk"] = {}
    try:
        disk_usage = shutil.disk_usage(".")
        data["hard_disk"] = {
            "total": disk_usage.total // 1024, # in kiB
            "used": disk_usage.used // 1024,
            "free": disk_usage.free // 1024,
        }
    except Exception as exc:
        # print(f"Error getting disk_usage from shutil: {e}", file=sys.stderr)
        data["hard_disk_scrape_error"] = repr(exc)

    try:
        data["os"] = run_cmd(
            'lsb_release -d | grep -Po "Description:\\s*\\K.*"'
        ).strip()
        data["virtualization"] = run_cmd("virt-what").strip()
    except Exception as exc:
        # print(f'Error getting os specs: {e}', flush=True)
        data["os_scrape_error"] = repr(exc)

    return data

# json dump to file
with open("/specs/specs.json", "w") as file:
    json.dump(scrape_specs(), file)

def hash(s: bytes) -> bytes:
    return b64encode(hashlib.sha256(s).digest(), altchars=b"-_")

with open("/volume/payload.txt", "rb") as file:
    data = pickle.load(file)

    answers = []
    for i in range(int(data["n"])):
        payload = data["payloads"][i]
        mask = data["masks"][i]
        algorithm = data["algorithms"][i]

        if i > 0:
            # decrypt payload with previous cracked passwords
            passwords = '\n'.join(answers[-1]).encode("utf-8")
            key = b64encode(hashlib.sha256(passwords).digest(), altchars=b"-_")
            payload = Fernet(key).decrypt(payload).decode("utf-8")

        with open("_payload.txt", mode="wb") as f:
            f.write(payload.encode("utf-8"))

        cmd = f'hashcat --potfile-disable --restore-disable --attack-mode 3 --workload-profile 3 --optimized-kernel-enable --hash-type {algorithm} --hex-salt -1 "?l?d?u" --outfile-format 2 --quiet _payload.txt "{mask}"'
        passwords = subprocess.check_output(cmd, shell=True, text=True)
        passwords = [p for p in sorted(passwords.split('\n')) if p != '']
        answers.append(passwords)

    print(
        hash("".join(["".join(passwords) for passwords in answers]).encode("utf-8")).decode("utf-8")
        )

