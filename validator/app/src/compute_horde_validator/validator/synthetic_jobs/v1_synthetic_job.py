import base64
import hashlib
import pickle
import random
import secrets
import string
from base64 import b64encode
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Self

from cryptography.fernet import Fernet

from compute_horde_validator.validator.synthetic_jobs.synthetic_job import (
    Algorithm,
    JobParams,
    SyntheticJob,
)

# pack 4 hotkey characters into 3 bytes
SALT_HOTKEY_NUM_CHARS = 4


@dataclass
class V1SyntheticJob(SyntheticJob):
    algorithms: list[Algorithm]
    passwords: list[list[str]]
    salts: list[bytes]
    params: list[JobParams]
    salt_prefix: str

    DIGITS: ClassVar[str] = string.digits
    ALPHABET: ClassVar[str] = string.ascii_letters + DIGITS

    @classmethod
    def random_string(cls, num_letters: int, num_digits: int) -> str:
        return "".join(random.choice(cls.ALPHABET) for _ in range(num_letters)) + "".join(
            random.choice(cls.DIGITS) for _ in range(num_digits)
        )

    @classmethod
    def generate(
        cls,
        algorithms: list[Algorithm],
        params: list[JobParams],
        salt_length_bytes: int = 8,
        salt_prefix: str | None = None,
    ) -> Self:
        # generate distinct passwords for each algorithm
        passwords = []
        for _params in params:
            _passwords = set()
            while len(_passwords) < _params.num_hashes:
                _passwords.add(
                    cls.random_string(
                        num_letters=_params.num_letters, num_digits=_params.num_digits
                    )
                )
            passwords.append(sorted(list(_passwords)))

        # generate salts
        salt_bytes_prefix = b""
        if salt_prefix:
            salt_prefix = salt_prefix[:SALT_HOTKEY_NUM_CHARS]
            salt_bytes_prefix = base64.b64decode(salt_prefix)
        salts = [
            salt_bytes_prefix + secrets.token_bytes(salt_length_bytes - len(salt_bytes_prefix))
            for _ in range(len(algorithms))
        ]

        return cls(
            algorithms=algorithms,
            params=params,
            passwords=passwords,
            salts=salts,
            salt_prefix=salt_prefix if salt_prefix else "",
        )

    @property
    def timeout_seconds(self) -> int:
        return max([p.timeout for p in self.params])

    def hash_masks(self) -> list[str]:
        return ["?1" * params.num_letters + "?d" * params.num_digits for params in self.params]

    def hash_hexes(self, i) -> list[str]:
        return [
            self.algorithms[i].hash(password.encode("ascii") + self.salts[i]).hexdigest()
            for password in self.passwords[i]
        ]

    def _hash(self, s: bytes) -> bytes:
        return b64encode(hashlib.sha256(s).digest(), altchars=b"-_")

    def _encrypt(self, key: str, payload: str) -> str:
        key_bytes = self._hash(key.encode("utf-8"))
        return Fernet(key_bytes).encrypt(payload.encode("utf-8")).decode("utf-8")

    def _payload(self, i) -> str:
        return "\n".join([f"{hash_hex}:{self.salts[i].hex()}" for hash_hex in self.hash_hexes(i)])

    def _payloads(self) -> list[str]:
        payloads = []
        for i in range(len(self.algorithms)):
            # start with unencrypted payload
            if i == 0:
                payloads.append(self._payload(i))
                continue
            # encrypt subsequent payloads with previous payload's passwords
            prev_passwords = "\n".join(self.passwords[i - 1])
            encrypted_payload = self._encrypt(prev_passwords, self._payload(i))
            payloads.append(encrypted_payload)
        return payloads

    @property
    def payload(self) -> str | bytes:
        """Convert this instance to a hashcat argument format."""

        data = {
            "n": len(self.algorithms),
            "payloads": self._payloads(),
            "masks": self.hash_masks(),
            "algorithms": [algorithm.type for algorithm in self.algorithms],
            "num_letters": [params.num_letters for params in self.params],
            "num_digits": [params.num_digits for params in self.params],
        }

        return pickle.dumps(data)

    def docker_run_cmd(self) -> list[str]:
        return ["/script.py"]

    def raw_script(self) -> str:
        with open(Path(__file__).parent / "v1_decrypt.py") as file:
            return file.read()

    @property
    def answer(self) -> str:
        passwords_str = "".join(["".join(passwords) for passwords in self.passwords])
        # if available, also verify the miner hotkey salt prefix as part of the answer
        passwords_str += self.salt_prefix
        return self._hash(passwords_str.encode("utf-8")).decode("utf-8")

    def __str__(self) -> str:
        return f"V1SyntheticJob {self.algorithms} {self.params}"


if __name__ == "__main__":
    import json

    from v1_decrypt import decrypt

    algorithms = Algorithm.get_all_algorithms()

    params = [
        JobParams(timeout=53, num_letters=2, num_digits=0, num_hashes=1),
        JobParams(timeout=53, num_letters=1, num_digits=1, num_hashes=1),
        JobParams(timeout=53, num_letters=1, num_digits=1, num_hashes=1),
    ]

    job = V1SyntheticJob.generate(algorithms, params, salt_prefix="5HBVrFGy6oYhRRtEE")
    # print(job.raw_script())

    data = pickle.loads(job.payload)
    print(f"Payload: {json.dumps(data, indent=4)}")

    answers = decrypt(data)
    print(f"Cracked Answer:  {answers}")
    print(f"Expected Answer: {job.answer}")
