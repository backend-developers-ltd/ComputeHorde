import logging

from compute_horde.signature import SignatureNotFound
from django.utils.deprecation import MiddlewareMixin

from ..services.signatures import signature_from_request

logger = logging.getLogger(__name__)


class FacilitatorSignatureMiddleware(MiddlewareMixin):
    """
    Middleware that extracts the signature from the request and saves it to the database
    """

    def process_request(self, request):
        try:
            request.signature = signature_from_request(request).model_dump()
        except SignatureNotFound:
            logger.warning("Request headers does not contain a signature")
            request.signature = None
            pass


def require_signature(request):
    if not getattr(request, "signature", None):
        raise SignatureNotFound("Request signature not found, but is required")
