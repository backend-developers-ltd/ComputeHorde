import pytest
from compute_horde_core.signature import SignatureNotFound, SignatureTimeoutException

from project.core.exception_handlers import api_exception_handler


@pytest.mark.parametrize(
    "exc, expected_response, expected_status",
    [
        (
            SignatureTimeoutException("timed out"),
            {
                "error": "Invalid signature provided",
                "details": "timed out",
            },
            400,
        ),
        (
            SignatureNotFound("msg"),
            {
                "error": "Signature not found",
                "details": "msg",
            },
            400,
        ),
    ],
)
def test_api_exception_handler(exc, expected_response, expected_status):
    response = api_exception_handler(exc, None)
    assert response.data == expected_response
    assert response.status_code == expected_status
