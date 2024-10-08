import datetime
import enum
import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol


class SyntheticJob(ABC):
    @property
    @abstractmethod
    def payload(self) -> str | bytes: ...

    @property
    @abstractmethod
    def answer(self) -> str: ...

    @property
    @abstractmethod
    def timeout_seconds(self) -> int: ...

    def docker_run_cmd(self) -> list[str]:
        return []

    def raw_script(self) -> str | None:
        return None


class _HashProto(Protocol):
    # "hashlib._Hash" could be used instead, but as it doesn't exist outside of stubs
    # PyCharm doesn't like it. This minimal protocol serves as a good enough stub.
    def hexdigest(self) -> str: ...


class Algorithm(enum.Enum):
    SHA256 = "SHA256"
    SHA384 = "SHA384"
    SHA512 = "SHA512"

    @property
    def params(self):
        return {
            Algorithm.SHA256: {
                "hash_function": hashlib.sha256,
                "hash_type": "1410",
            },
            Algorithm.SHA384: {
                "hash_function": hashlib.sha384,
                "hash_type": "10810",
            },
            Algorithm.SHA512: {
                "hash_function": hashlib.sha512,
                "hash_type": "1710",
            },
        }

    def hash(self, *args, **kwargs) -> _HashProto:
        return self.params[self]["hash_function"](*args, **kwargs)  # type: ignore

    @property
    def type(self):
        return self.params[self]["hash_type"]

    @classmethod
    def get_random_algorithm(cls):
        algorithms = cls.get_all_algorithms()
        return algorithms[datetime.datetime.utcnow().hour % len(algorithms)]

    @classmethod
    def get_all_algorithms(cls):
        return list(Algorithm)


@dataclass
class JobParams:
    timeout: int
    num_letters: int
    num_digits: int
    num_hashes: int

    @property
    def password_length(self) -> int:
        return self.num_letters + self.num_digits

    def __str__(self) -> str:
        return f"timeout={self.timeout} num_letters={self.num_letters} num_digits={self.num_digits} num_hashes={self.num_hashes}"


HASHJOB_PARAMS = {
    0: {
        Algorithm.SHA256: JobParams(timeout=90, num_letters=6, num_digits=0, num_hashes=1),
        Algorithm.SHA384: JobParams(timeout=45, num_letters=5, num_digits=0, num_hashes=1),
        Algorithm.SHA512: JobParams(timeout=45, num_letters=5, num_digits=0, num_hashes=1),
    },
    1: {
        Algorithm.SHA256: JobParams(timeout=53, num_letters=6, num_digits=0, num_hashes=100),
        Algorithm.SHA384: JobParams(timeout=53, num_letters=5, num_digits=1, num_hashes=100),
        Algorithm.SHA512: JobParams(timeout=53, num_letters=5, num_digits=1, num_hashes=100),
    },
}
HASHJOB_PARAMS[2] = HASHJOB_PARAMS[1]
HASHJOB_PARAMS[3] = HASHJOB_PARAMS[1]
HASHJOB_PARAMS[4] = HASHJOB_PARAMS[1]
