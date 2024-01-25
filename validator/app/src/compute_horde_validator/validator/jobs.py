import hashlib
import secrets
import string
from dataclasses import dataclass
from typing import ClassVar, Self
from abc import ABC, abstractmethod


class SyntheticJob(ABC):
    @property
    @abstractmethod
    def payload(self) -> str:
        ...

    @property
    @abstractmethod
    def answer(self) -> str:
        ...


@dataclass
class V0SyntheticJob(SyntheticJob):
    password: str
    salt: str

    ALPHABET: ClassVar[str] = string.ascii_letters + string.digits

    @classmethod
    def random_string(cls, length: int) -> str:
        return ''.join(secrets.choice(cls.ALPHABET) for i in range(length))

    @classmethod
    def generate(cls, password_length: int = 6, salt_length_bytes: int = 8) -> Self:
        return cls(
            password=cls.random_string(password_length),
            salt=secrets.token_bytes(salt_length_bytes),
        )

    @property
    def hash_hex(self) -> str:
        return hashlib.sha256(self.password.encode('ascii') + self.salt).hexdigest()

    @property
    def payload(self) -> str:
        """ Convert this instance to a hashcat argument format. """
        return f'{self.hash_hex}:{self.salt.hex()}'

    @property
    def answer(self) -> str:
        return self.password


if __name__ == '__main__':
    job = V0SyntheticJob.generate()
    print(f'Payload: {job.payload}')
    print(f'Answer: {job.payload}:{job.answer}')
