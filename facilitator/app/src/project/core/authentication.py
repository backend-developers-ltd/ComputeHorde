import jwt
from django.conf import settings
from django.contrib.auth.models import AbstractBaseUser, AnonymousUser
from django.http import HttpRequest
from rest_framework import authentication
from rest_framework.exceptions import AuthenticationFailed
from structlog import get_logger

from .models import HotkeyWhitelist, Miner, Validator

log = get_logger(__name__)


def is_hotkey_allowed(hotkey: str) -> bool:
    return HotkeyWhitelist.objects.filter(ss58_address=hotkey).exists() and \
           (Validator.objects.filter(ss58_address=hotkey, is_active=True) or
            Miner.objects.filter(ss58_address=hotkey, is_active=True)
            )


class JWTAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request: HttpRequest) -> tuple[AbstractBaseUser, str] | None:
        auth_header = authentication.get_authorization_header(request)
        if not auth_header:
            return None

        try:
            auth_data = auth_header.decode("utf-8")
        except Exception:
            raise AuthenticationFailed("Invalid token header encoding.")

        try:
            prefix, token = auth_data.split(" ")
        except Exception:
            raise AuthenticationFailed("Invalid token header format.")

        if prefix.lower() != "bearer":
            return None

        try:
            payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        except jwt.ExpiredSignatureError:
            raise AuthenticationFailed("Token expired.")
        except jwt.DecodeError:
            raise AuthenticationFailed("Token decode error.")

        hotkey = payload.get("sub")
        if not hotkey:
            raise AuthenticationFailed("Token missing subject.")

        if not is_hotkey_allowed(hotkey):
            raise AuthenticationFailed("Unauthorized hotkey.")

        request.hotkey = hotkey
        return AnonymousUser(), hotkey
