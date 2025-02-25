import json

from compute_horde.fv_protocol.facilitator_requests import SignedFields
from compute_horde.signature import VERIFIERS_REGISTRY, Signature, SignatureInvalidException, signature_from_headers
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
    try:
        verifier = VERIFIERS_REGISTRY.get(signature.signature_type)
    except KeyError:
        raise SignatureInvalidException(f"Invalid signature type: {signature.signature_type}")
    try:
        json_body = json.loads(request.body)
    except ValueError:
        json_body = None

    signed_fields = SignedFields.from_facilitator_sdk_json(json_body)
    verifier.verify(signed_fields.model_dump_json(), signature)
    return signature
