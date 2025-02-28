import pytest
from compute_horde.fv_protocol.facilitator_requests import Signature
from compute_horde.signature import (
    BittensorWalletVerifier,
    SignatureInvalidException,
    signature_to_headers,
)
from compute_horde_core.signature import (
    SignatureScope,
)
from django.utils.datastructures import CaseInsensitiveMapping

from project.core.services.signatures import signature_from_request


@pytest.fixture
def request_json():
    return {"key": "value"}


@pytest.fixture
def mock_request(rf, request_json):
    request = rf.post(
        "/test-url/",
        data=request_json,
        content_type="application/json",
    )
    return request


@pytest.fixture
def verifier():
    return BittensorWalletVerifier()


@pytest.fixture
def payload_from_request(verifier, request_json):
    return verifier.payload_from_request(method="POST", url="/test-url/", headers={}, json=request_json)


@pytest.fixture
def signature(keypair, payload_from_request):
    signature = Signature(
        signature_type="bittensor",
        signatory=keypair.ss58_address,
        timestamp_ns=1719427963187622189,
        signature=b"nDJOrHD/NuNVCagHLmi0sSa4yMRkQOs9pgUUmtAEwGZuazLyc+boJgvNlgLwsezDpWo7r9GPdh7yEhpR3V3whA==",
    )
    # This signature was generated using the following code:
    # signer = SIGNERS_REGISTRY.get("bittensor", keypair)
    # signature = signer.sign(payload=payload_from_request)
    # but its hardcoded since we want to check for backwards compatibility
    return signature


@pytest.fixture
def mock_request_with_signature(mock_request, signature):
    mock_request.headers = CaseInsensitiveMapping(
        {**mock_request.headers, **signature_to_headers(signature, SignatureScope.SignedFields)}
    )
    return mock_request


def test_signature_from_request__invalid_signature(mock_request_with_signature):
    mock_request_with_signature.headers = CaseInsensitiveMapping(
        {**mock_request_with_signature.headers, **{"X-CH-Signature-Type": "invalid"}}
    )
    with pytest.raises(SignatureInvalidException):
        signature_from_request(mock_request_with_signature)
