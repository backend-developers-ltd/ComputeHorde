import secrets
import string
from dataclasses import dataclass
from typing import ClassVar, Self

from compute_horde_validator.validator.synthetic_jobs.synthetic_job import (
    HASHJOB_PARAMS,
    Algorithm,
    JobParams,
    SyntheticJob,
)


@dataclass
class V0SyntheticJob(SyntheticJob):
    algorithm: Algorithm
    password: str
    salt: bytes
    params: JobParams

    ALPHABET: ClassVar[str] = string.ascii_letters + string.digits

    @classmethod
    def random_string(cls, length: int) -> str:
        return "".join(secrets.choice(cls.ALPHABET) for _ in range(length))

    @classmethod
    def generate(cls, algorithm: Algorithm, params: JobParams, salt_length_bytes: int = 8) -> Self:
        return cls(
            algorithm=algorithm,
            password=cls.random_string(params.password_length),
            params=params,
            salt=secrets.token_bytes(salt_length_bytes),
        )

    @property
    def timeout_seconds(self) -> int:
        return self.params.timeout

    @property
    def hash_hex(self) -> str:
        return self.algorithm.hash(self.password.encode("ascii") + self.salt).hexdigest()

    @property
    def payload(self) -> str:
        """Convert this instance to a hashcat argument format."""
        return f"{self.hash_hex}:{self.salt.hex()}"

    @property
    def answer(self) -> str:
        return self.password

    def docker_run_cmd(self) -> list[str]:
        return [
            "--runtime",
            "600",
            "--restore-disable",
            "--attack-mode",
            "3",
            "--workload-profile",
            "3",
            "--optimized-kernel-enable",
            "--hash-type",
            self.algorithm.type,
            "--hex-salt",
            "-1",
            "?l?d?u",
            "--outfile-format",
            "2",
            "--quiet",
            "/volume/payload.txt",
            "?1" * len(self.password),
        ]

    def __str__(self) -> str:
        return f"{self.algorithm.value}, password length = {self.params.password_length}, timeout={self.timeout_seconds}s"


if __name__ == "__main__":
    algorithm = Algorithm.SHA256
    job = V0SyntheticJob.generate(algorithm, HASHJOB_PARAMS[0][algorithm])
    print(f"Payload: {job.payload}")
    print(f"Answer: {job.payload}:{job.answer}")
