import hashlib
import pickle
import subprocess
from base64 import b64encode
from sys import argv

from cryptography.fernet import Fernet


def run_cmd(cmd):
    proc = subprocess.run(cmd, shell=True, capture_output=True, check=False, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"run_cmd error {cmd=!r} {proc.returncode=} {proc.stdout=!r} {proc.stderr=!r}"
        )
    return proc.stdout


def hash(s: bytes) -> bytes:
    return b64encode(hashlib.sha256(s).digest(), altchars=b"-_")


with open("/volume/payload.txt", "rb") as file:
    first_key = argv[1]
    data = pickle.load(file)

    answers: list[list[str]] = []
    for i in range(int(data["n"])):
        payload = data["payloads"][i]
        mask = data["masks"][i]
        algorithm = data["algorithms"][i]

        if i == 0:
            # decrypt first payload with received key
            key = b64encode(hashlib.sha256(first_key.encode("utf-8")).digest(), altchars=b"-_")
        else:
            # decrypt payload with previous cracked passwords
            previous_passwords = "\n".join(answers[-1]).encode("utf-8")
            key = b64encode(hashlib.sha256(previous_passwords).digest(), altchars=b"-_")

        payload = Fernet(key).decrypt(payload).decode("utf-8")

        with open("_payload.txt", mode="wb") as f:
            f.write(payload.encode("utf-8"))

        cmd = f'hashcat --potfile-disable --restore-disable --attack-mode 3 --workload-profile 3 --optimized-kernel-enable --hash-type {algorithm} --hex-salt -1 "?l?d?u" --outfile-format 2 --quiet _payload.txt "{mask}"'
        output = subprocess.check_output(cmd, shell=True, text=True)
        passwords = [p for p in sorted(output.split("\n")) if p != ""]
        answers.append(passwords)

    # The answer to the job is the whole stdout, so this should be the only thing that prints anything in this script.
    print(
        hash("".join(["".join(passwords) for passwords in answers]).encode("utf-8")).decode("utf-8")
    )
