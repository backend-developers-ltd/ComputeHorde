import enum
import hashlib
import secrets
import string
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar, Self


class SyntheticJob(ABC):
    @property
    @abstractmethod
    def payload(self) -> str:
        ...

    @property
    @abstractmethod
    def answer(self) -> str:
        ...


class Algorithm(enum.Enum):
    SHA256 = "SHA256"
    SHA384 = "SHA384"
    SHA512 = "SHA512"


@dataclass
class V0SyntheticJob(SyntheticJob):
    algorithm: Algorithm
    password: str
    salt: bytes

    ALPHABET: ClassVar[str] = string.ascii_letters + string.digits

    @classmethod
    def random_string(cls, length: int) -> str:
        return ''.join(secrets.choice(cls.ALPHABET) for i in range(length))

    @classmethod
    def generate(cls, algorithm: Algorithm, password_length: int = 6, salt_length_bytes: int = 8) -> Self:
        return cls(
            algorithm=algorithm,
            password=cls.random_string(password_length),
            salt=secrets.token_bytes(salt_length_bytes),
        )

    @property
    def hash_hex(self) -> str:
        if self.algorithm == Algorithm.SHA256:
            hash_function = hashlib.sha256
        elif self.algorithm == Algorithm.SHA384:
            hash_function = hashlib.sha384
        elif self.algorithm == Algorithm.SHA512:
            hash_function = hashlib.sha512
        else:
            raise ValueError(f'Unsupported algorithm {self.algorithm}')
        return hash_function(self.password.encode('ascii') + self.salt).hexdigest()

    @property
    def payload(self) -> str:
        """ Convert this instance to a hashcat argument format. """
        return f'{self.hash_hex}:{self.salt.hex()}'

    @property
    def answer(self) -> str:
        return self.password


if __name__ == '__main__':
    job = V0SyntheticJob.generate(Algorithm.SHA256)
    print(f'Payload: {job.payload}')
    print(f'Answer: {job.payload}:{job.answer}')
