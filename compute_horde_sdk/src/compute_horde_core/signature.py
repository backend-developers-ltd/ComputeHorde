import abc
import base64
import datetime
import hashlib
import json
import re
import sys
import time
import typing
from typing import ClassVar, Protocol

import bittensor_wallet
from pydantic import BaseModel, JsonValue, field_serializer, field_validator

from compute_horde_core.streaming import StreamingDetails

if sys.version_info >= (3, 11):  # noqa: UP036
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  # noqa: UP035


class SignatureScope(StrEnum):
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


class SignedRequest:
    signature: Signature | None = None

    @abc.abstractmethod
    def get_signed_payload(self) -> JsonValue:
        """Return payload to be signed"""
        pass


class SignedFields(BaseModel):
    executor_class: str
    docker_image: str
    args: list[str]
    env: dict[str, str]
    use_gpu: bool
    artifacts_dir: str
    on_trusted_miner: bool
    download_time_limit: int
    execution_time_limit: int
    upload_time_limit: int
    streaming_start_time_limit: int
    streaming_details: StreamingDetails | None = None

    volumes: list[JsonValue]
    uploads: list[JsonValue]

    @staticmethod
    def from_facilitator_sdk_json(data: JsonValue) -> "SignedFields":
        data = typing.cast(dict[str, JsonValue], data)

        signed_fields = SignedFields(
            executor_class=str(data.get("executor_class")),
            docker_image=str(data.get("docker_image", "")),
            args=typing.cast(list[str], data.get("args", [])),
            env=typing.cast(dict[str, str], data.get("env", {})),
            use_gpu=typing.cast(bool, data.get("use_gpu", False)),
            volumes=typing.cast(list[JsonValue], data.get("volumes", [])),
            uploads=typing.cast(list[JsonValue], data.get("uploads", [])),
            artifacts_dir=typing.cast(str, data.get("artifacts_dir") or ""),
            on_trusted_miner=typing.cast(bool, data.get("on_trusted_miner", False)),
            download_time_limit=typing.cast(int, data.get("download_time_limit", 0)),
            execution_time_limit=typing.cast(int, data.get("execution_time_limit", 0)),
            upload_time_limit=typing.cast(int, data.get("upload_time_limit", 0)),
            streaming_start_time_limit=typing.cast(int, data.get("streaming_start_time_limit", 0)),
            streaming_details=StreamingDetails.model_validate(data["streaming_details"])
            if "streaming_details" in data
            else None,
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
    ) -> JsonValue:
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
    ) -> None:
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
    def __init__(self, wallet: bittensor_wallet.Wallet | bittensor_wallet.Keypair | None = None):
        if isinstance(wallet, bittensor_wallet.Keypair):
            keypair = wallet
        else:
            keypair = (wallet or bittensor_wallet.Wallet()).hotkey
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
            keypair = bittensor_wallet.Keypair(ss58_address=signature.signatory)
        except ValueError:
            raise SignatureInvalidException("Invalid signatory for BittensorWalletVerifier")
        try:
            if not keypair.verify(data=payload, signature=signature.signature):
                raise SignatureInvalidException("Signature is invalid")
        except (ValueError, TypeError) as e:
            raise SignatureInvalidException("Signature is malformed") from e


class SignatureExtractor(Protocol):
    def __call__(self, headers: dict[str, str], prefix: str = "") -> Signature: ...


def verify_signature(
    payload: JsonValue | bytes,
    signature: Signature,
    *,
    newer_than: datetime.datetime | None = None,
) -> None:
    """
    Verifies the signature of the payload

    :param payload: payload to be verified
    :param signature: signature object
    :param newer_than: if provided, checks if the signature is newer than the provided timestamp
    :return: None
    :raises SignatureInvalidException: if the signature is invalid
    """
    verifier = BittensorWalletVerifier()
    verifier.verify(payload, signature, newer_than)


def verify_request(
    method: str,
    url: str,
    headers: dict[str, str],
    json: JsonValue | None = None,
    *,
    newer_than: datetime.datetime | None = None,
    signature_extractor: SignatureExtractor = signature_from_headers,
) -> Signature | None:
    """
    Verifies the signature of the request

    :param method: HTTP method
    :param url: request URL
    :param headers: request headers
    :param json: request JSON payload
    :param newer_than: if provided, checks if the signature is newer than the provided timestamp
    :param signature_extractor: function to extract the signature from the headers
    :return: Signature object or None if no signature found
    :raises SignatureInvalidException: if the signature is invalid
    """
    try:
        signature = signature_extractor(headers)
    except SignatureNotFound:
        return None
    verifier = BittensorWalletVerifier()
    payload = verifier.payload_from_request(method, url, headers=headers, json=json)
    verifier.verify(payload, signature, newer_than)
    return signature
