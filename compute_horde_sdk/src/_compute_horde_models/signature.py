import abc
import base64
import datetime
import hashlib
import json
import re
import time
import typing
from enum import Enum
from typing import ClassVar

import bittensor
from pydantic import BaseModel, JsonValue, field_serializer, field_validator


class SignatureScope(Enum):
    SignedFields = "SignedFields"
    FullRequest = "FullRequest"


class Signature(BaseModel, extra="forbid"):
    # has defaults to allow easy instantiation
    signature_type: str = ""
    signatory: str = ""  # identity of the signer (e.g. sa58 address if signature_type == "bittensor")
    timestamp_ns: int = 0  # UNIX timestamp in nanoseconds
    signature: bytes
    signature_scope: SignatureScope = SignatureScope.SignedFields

    @field_validator("signature")
    @classmethod
    def validate_signature(cls, signature: str) -> bytes:
        return base64.b64decode(signature)

    @field_serializer("signature")
    def serialize_signature(self, signature: bytes) -> str:
        return base64.b64encode(signature).decode("utf-8")


class SignedFields(BaseModel):
    executor_class: str
    docker_image: str
    raw_script: str
    args: list[str]
    env: dict[str, str]
    use_gpu: bool
    artifacts_dir: str

    volumes: list[JsonValue]
    uploads: list[JsonValue]

    @staticmethod
    def from_facilitator_sdk_json(data: JsonValue):
        data = typing.cast(dict[str, JsonValue], data)

        signed_fields = SignedFields(
            executor_class=str(data.get("executor_class")),
            docker_image=str(data.get("docker_image", "")),
            raw_script=str(data.get("raw_script", "")),
            args=typing.cast(list[str], data.get("args", [])),
            env=typing.cast(dict[str, str], data.get("env", None)),
            use_gpu=typing.cast(bool, data.get("use_gpu")),
            volumes=typing.cast(list[JsonValue], data.get("volumes", [])),
            uploads=typing.cast(list[JsonValue], data.get("uploads", [])),
            artifacts_dir=typing.cast(str, data.get("artifacts_dir") or ""),
        )
        return signed_fields


def signature_from_headers(headers: dict[str, str], prefix: str = "X-CH-") -> Signature:
    """
    Extracts the signature from the headers

    :param headers: headers dict
    :return: Signature object
    """
    try:
        return Signature(
            signature_type=headers[f"{prefix}Signature-Type"],
            signatory=headers[f"{prefix}Signatory"],
            timestamp_ns=int(headers[f"{prefix}Timestamp-NS"]),
            signature=headers[f"{prefix}Signature"].encode("utf-8"),
            signature_scope=SignatureScope(headers.get(f"{prefix}Signature-Scope", SignatureScope.SignedFields.name)),
        )
    except (
        KeyError,
        ValueError,
        TypeError,
    ) as e:
        raise SignatureNotFound("Signature not found in headers") from e


def signature_to_headers(signature: Signature, scope: SignatureScope, prefix: str = "X-CH-") -> dict[str, str]:
    """
    Converts the signature to headers

    :param signature: Signature object
    :return: headers dict
    """
    return {
        f"{prefix}Signature-Type": signature.signature_type,
        f"{prefix}Signatory": signature.signatory,
        f"{prefix}Timestamp-NS": str(signature.timestamp_ns),
        f"{prefix}Signature": base64.b64encode(signature.signature).decode("utf-8"),
        f"{prefix}Signature-Scope": scope.name,
    }


class SignatureException(Exception):
    pass


class SignatureNotFound(SignatureException):
    pass


class SignatureInvalidException(SignatureException):
    pass


class SignatureTimeoutException(SignatureInvalidException):
    pass


def hash_message_signature(payload: bytes | JsonValue, signature: Signature) -> bytes:
    """
    Hashes the message to be signed with the signature parameters

    :param payload: payload to be signed
    :param signature: incomplete signature object with Signature parameters
    :return:
    """
    if not isinstance(payload, bytes):
        payload = json.dumps(payload, sort_keys=True).encode("utf-8")

    hasher = hashlib.blake2b()
    hasher.update(signature.timestamp_ns.to_bytes(8, "big"))
    hasher.update(payload)
    return hasher.digest()


_REMOVE_URL_SCHEME_N_HOST_RE = re.compile(r"^\w+://[^/]+")


def signature_payload(method: str, url: str, headers: dict[str, str], json: JsonValue | None = None) -> JsonValue:
    reduced_url = _REMOVE_URL_SCHEME_N_HOST_RE.sub("", url)
    return {
        "action": f"{method.upper()} {reduced_url}",
        "json": json,
    }


class SignatureScheme(abc.ABC):
    signature_type: ClassVar[str]

    def payload_from_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        json: JsonValue | None = None,
    ):
        return signature_payload(
            method=method,
            url=url,
            headers=headers,
            json=json,
        )


class Signer(SignatureScheme):
    def sign(self, payload: JsonValue | bytes) -> Signature:
        signature = Signature(
            signature_type=self.signature_type,
            signatory=self.get_signatory(),
            timestamp_ns=time.time_ns(),
            signature=b"",
        )
        payload_hash = hash_message_signature(payload, signature)
        signature.signature = self._sign(payload_hash)
        return signature

    def signature_for_request(
        self, method: str, url: str, headers: dict[str, str], json: JsonValue | None = None
    ) -> Signature:
        return self.sign(self.payload_from_request(method, url, headers=headers, json=json))

    @abc.abstractmethod
    def _sign(self, payload: bytes) -> bytes:
        raise NotImplementedError

    @abc.abstractmethod
    def get_signatory(self) -> str:
        raise NotImplementedError


class Verifier(SignatureScheme):
    def verify(
        self,
        payload: JsonValue | bytes,
        signature: Signature,
        newer_than: datetime.datetime | None = None,
    ):
        payload_hash = hash_message_signature(payload, signature)
        self._verify(payload_hash, signature)

        if newer_than is not None:
            if newer_than > datetime.datetime.fromtimestamp(signature.timestamp_ns / 1_000_000_000):
                raise SignatureTimeoutException("Signature is too old")

    @abc.abstractmethod
    def _verify(self, payload: bytes, signature: Signature) -> None:
        raise NotImplementedError


class BittensorSignatureScheme:
    signature_type = "bittensor"


class BittensorWalletSigner(BittensorSignatureScheme, Signer):
    def __init__(self, wallet: bittensor.wallet | bittensor.Keypair | None = None):
        if isinstance(wallet, bittensor.Keypair):
            keypair = wallet
        else:
            keypair = (wallet or bittensor.wallet()).hotkey
        self._keypair = keypair

    def _sign(self, payload: bytes) -> bytes:
        signature: bytes = self._keypair.sign(payload)
        return signature

    def get_signatory(self) -> str:
        signatory: str = self._keypair.ss58_address
        return signatory


class BittensorWalletVerifier(BittensorSignatureScheme, Verifier):
    def _verify(self, payload: bytes, signature: Signature) -> None:
        try:
            keypair = bittensor.Keypair(ss58_address=signature.signatory)
        except ValueError:
            raise SignatureInvalidException("Invalid signatory for BittensorWalletVerifier")
        try:
            if not keypair.verify(data=payload, signature=signature.signature):
                raise SignatureInvalidException("Signature is invalid")
        except (ValueError, TypeError) as e:
            raise SignatureInvalidException("Signature is malformed") from e
