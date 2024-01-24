import hashlib
import secrets
import string
from dataclasses import dataclass
from typing import ClassVar, Self


@dataclass
class HashChallenge:
    password: str
    salt: str

    ALPHABET: ClassVar[str] = string.ascii_letters + string.digits

    @classmethod
    def random_string(cls, length: int) -> str:
        return ''.join(secrets.choice(cls.ALPHABET) for i in range(length))

    @classmethod
    def generate(cls, password_length: int, salt_length_bytes: int = 8) -> Self:
        return HashChallenge(
            password=cls.random_string(password_length),
            salt=secrets.token_bytes(salt_length_bytes),
        )

    @property
    def hash_hex(self) -> str:
        return hashlib.sha256(self.password.encode('ascii') + self.salt).hexdigest()

    def as_hashcat(self) -> str:
        """ Convert this instance to a hashcat argument format. """
        return f'{self.hash_hex}:{self.salt.hex()}'


if __name__ == '__main__':
    challenge = HashChallenge.generate(password_length=6)
    print(f'Password: {challenge.password}')
    print(f'Challenge: {challenge.as_hashcat()}')
