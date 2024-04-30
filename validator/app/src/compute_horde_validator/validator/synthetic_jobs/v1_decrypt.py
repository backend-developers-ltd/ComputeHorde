import csv
import hashlib
import json
import pickle
import subprocess
from base64 import b64encode

from cryptography.fernet import Fernet


def run_cmd(cmd):
    try:
        return subprocess.check_output(['bash', '-c', cmd]).decode()
    except Exception:
        # print(f'Error running command {cmd}: {e}', flush=True)
        return ""

def scrape_specs() -> dict[str, any]:
    data: dict[str, any] = {}

    try:
        nvidia_cmd = run_cmd('nvidia-smi --query-gpu=name,driver_version,name,memory.total,compute_cap,power.limit,clocks.gr,clocks.mem --format=csv').splitlines()
        csv_data = csv.reader(nvidia_cmd)
        header = [x.strip() for x in next(csv_data)]
        gpus = []
        for row in csv_data:
            row = [x.strip() for x in row]
            gpu_data = dict(zip(header, row))
            print(gpu_data)
            gpus.append({
                'name': gpu_data["name"],
                'driver': gpu_data["driver_version"],
                'capacity': gpu_data["memory.total [MiB]"].split(' ')[0],
                'cuda': gpu_data["compute_cap"],
                'power_limit': gpu_data["power.limit [W]"].split(' ')[0],
                'graphics_speed': gpu_data["clocks.current.graphics [MHz]"].split(' ')[0],
                'memory_speed': gpu_data["clocks.current.memory [MHz]"].split(' ')[0],
            })
        print(gpus)
        data['gpu'] = {
                'details': gpus,
                'count': len(gpus)
                }
    except Exception:
        # print(f'Error processing scraped gpu specs: {e}', flush=True)
        pass
    try:
        data['os'] = run_cmd('lsb_release -d | grep -Po \"Description:\\s*\\K.*\"').strip()
        data['virtualization'] = run_cmd('virt-what').strip()

        data['ram'] = {}
        data['ram']['total'] = run_cmd('cat /proc/meminfo | grep -P "MemTotal" | grep -o \"[0-9]*\"').strip()
        data['ram']['used'] = run_cmd('cat /proc/meminfo | grep -P "MemUsed" | grep -o \"[0-9]*\"').strip()
        data['ram']['free'] = run_cmd('cat /proc/meminfo | grep -P "MemFree" | grep -o \"[0-9]*\"').strip()
        data['ram']['available'] = run_cmd('cat /proc/meminfo | grep -P "MemAvailable" | grep -o \"[0-9]*\"').strip()
        data['hard_disk'] = {}
        data['hard_disk']['total'] = run_cmd('df . -P | sed -n 2p  | cut -d \' \' -f 9').strip()
        data['hard_disk']['used'] = run_cmd('df . -P | sed -n 2p  | cut -d \' \' -f 10').strip()
        data['hard_disk']['free'] = run_cmd('df . -P | sed -n 2p  | cut -d \' \' -f 13').strip()

        data['cpu'] = {}
        data['cpu']['model'] = run_cmd('lscpu | grep -Po \"^Model name:\\s*\\K.*\"').strip()
        data['cpu']['count'] = run_cmd('lscpu | grep -Po \"^CPU\\(s\\):\\s*\\K.*\"').strip()

        cpu_data = run_cmd('lscpu --parse=MHZ | grep -Po \"^[0-9,.]*$\"').splitlines()
        data['cpu']['clocks'] = [ float(x) for x in cpu_data ]
    except Exception:
        # print(f'Error processing scraped specs: {e}', flush=True)
        pass
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

