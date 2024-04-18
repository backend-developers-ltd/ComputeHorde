import hashlib
import pickle
import subprocess
from base64 import b64encode

from cryptography.fernet import Fernet

with open("/volume/payload.txt", "rb") as file:
    data = pickle.load(file)

    answers = []
    for i in range(int(data["n"])):
        payload = data["payloads"][i]
        mask = data["masks"][i]
        algorithm = data["algorithms"][i]
        timeout = data["timeout"][i]

        if i > 0:
            # decrypt payload with previous cracked passwords
            passwords = '\n'.join(answers[-1]).encode("utf-8")
            key = b64encode(hashlib.sha256(passwords).digest(), altchars=b"-_")
            payload = Fernet(key).decrypt(payload).decode("utf-8")

        with open("_payload.txt", mode="wb") as f:
            f.write(payload.encode("utf-8"))

        cmd = f'hashcat --runtime {timeout} --potfile-disable --restore-disable --attack-mode 3 --workload-profile 3 --optimized-kernel-enable --hash-type {algorithm} --hex-salt -1 "?l?d?u" --outfile-format 2 --quiet _payload.txt "{mask}"'
        passwords = subprocess.check_output(cmd, shell=True, text=True)
        passwords = sorted(passwords.split("\\n"))[1:]
        answers.append(passwords)
    print(answers)
