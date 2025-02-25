class FacilitatorClientException(Exception):
    pass


class FacilitatorClientTimeoutException(FacilitatorClientException, TimeoutError):
    pass


class SignatureRequiredException(FacilitatorClientException):
    pass
