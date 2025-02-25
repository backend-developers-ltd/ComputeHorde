from compute_horde.signature import SignatureInvalidException, SignatureNotFound
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import exception_handler as drf_exception_handler


def api_exception_handler(exc, context):
    response = drf_exception_handler(exc, context)

    match exc:
        case SignatureInvalidException():
            response = Response(
                {"error": "Invalid signature provided", "details": str(exc)},
                status=status.HTTP_400_BAD_REQUEST,
            )
        case SignatureNotFound():
            response = Response(
                {"error": "Signature not found", "details": str(exc)},
                status=status.HTTP_400_BAD_REQUEST,
            )

    return response
