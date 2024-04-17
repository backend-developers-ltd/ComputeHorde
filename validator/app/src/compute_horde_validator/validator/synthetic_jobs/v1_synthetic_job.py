import hashlib
import pickle
import secrets
import string
from base64 import b64encode
from dataclasses import dataclass
from typing import ClassVar, Self

from app.src.compute_horde_validator.validator.synthetic_jobs.synthetic_job import (
    HASHJOB_PARAMS,
    Algorithm,
    JobParams,
    SyntheticJob,
)
from app.src.compute_horde_validator.validator.synthetic_jobs.v1_decrypt import DECRYPT_SCRIPT
from cryptography.fernet import Fernet


@dataclass
class V1SyntheticJob(SyntheticJob):
    algorithms: list[Algorithm]
    passwords: list[list[str]]
    salts: list[bytes]
    params: list[JobParams]

    DIGITS: ClassVar[str] = string.digits
    ALPHABET: ClassVar[str] = string.ascii_letters + DIGITS

    @classmethod
    def random_string(cls, num_digits: int, num_letters: int) -> str:
        return "".join(secrets.choice(cls.ALPHABET) for _ in range(num_letters)) + "".join(
            secrets.choice(cls.DIGITS) for _ in range(num_digits)
        )


    @classmethod
    def generate(
        cls, algorithms: list[Algorithm], params: list[JobParams], salt_length_bytes: int = 8
    ) -> Self:

        # generate distinct passwords for each algorithm
        passwords = []
        for _params in params:
            _passwords = set()
            while len(_passwords) < _params.num_hashes:
                _passwords.add(cls.random_string(_params.num_letters, _params.num_digits))
            passwords.append(sorted(list(_passwords)))

        return cls(
            algorithms=algorithms,
            params=params,
            passwords=passwords,
            salts=[secrets.token_bytes(salt_length_bytes) for _ in range(len(algorithms))],
        )

    @property
    def timeout_seconds(self) -> int:
        return sum([p.timeout for p in self.params])

    def hash_masks(self) -> list[str]:
        return ["?1" * params.num_letters + "?d" * params.num_digits for params in self.params]

    def hash_hexes(self, i) -> list[str]:
        return [
            self.algorithms[i].hash(password.encode("ascii") + self.salts[i]).hexdigest()
            for password in self.passwords[i]
        ]

    def _encrypt(self, key, payload) -> bytes:
        key = b64encode(hashlib.sha256(key).digest(), altchars=b"-_")
        return Fernet(key).encrypt(payload.encode("utf-8"))

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
            prev_passwords = "\n".join(self.passwords[i - 1]).encode("utf-8")
            encrypted_payload = self._encrypt(prev_passwords, self._payload(i)).decode("utf-8")
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
        }
        return pickle.dumps(data)

    def docker_run_cmd(self) -> list[str]:
        return [ "/script.py" ]

    def raw_script(self) -> str:
        return DECRYPT_SCRIPT

    @property
    def answer(self) -> str:
        return str(self.passwords)


if __name__ == "__main__":
    algorithms = Algorithm.get_all_algorithms()
    params = [HASHJOB_PARAMS[1][algorithm] for algorithm in algorithms]
    job = V1SyntheticJob.generate(algorithms, params)
    # print(job.raw_script())
    print(f"Payload: {job.payload}")
    print(f"Answer: {job.answer}")
