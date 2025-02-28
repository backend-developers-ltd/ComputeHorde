import json

from compute_horde.signature import (
    BittensorWalletVerifier,
    SignatureInvalidException,
    signature_from_headers,
)
from compute_horde_core.signature import Signature, SignatureScope, SignedFields
from django.http import HttpRequest


def signature_from_request(request: HttpRequest) -> Signature:
    """
    Extracts the signature from the request and verifies it.

    :param request: HttpRequest object
    :return: Signature from the request
    :raises SignatureNotFound: if the signature is not found in the request
    :raises SignatureInvalidException: if the signature is invalid
    """
    signature = signature_from_headers(request.headers)
    verifier = BittensorWalletVerifier()
    try:
        json_body = json.loads(request.body)
    except ValueError:
        json_body = None

    if signature.signature_scope == SignatureScope.SignedFields:
        signed_fields = SignedFields.from_facilitator_sdk_json(json_body)
        verifier.verify(signed_fields.model_dump_json(), signature)
    elif signature.signature_scope == SignatureScope.FullRequest:
        signed_fields = json.dumps(json_body, sort_keys=True)
        verifier.verify(signed_fields, signature)
    else:
        raise SignatureInvalidException(f"Invalid signature scope: {signature}")

    return signature
