import jwt
from django.conf import settings
from django.contrib.auth.models import AbstractBaseUser, User
from django.http import HttpRequest
from rest_framework import authentication
from rest_framework.exceptions import AuthenticationFailed
from structlog import get_logger

from .models import HotkeyWhitelist, Miner, Validator

log = get_logger(__name__)


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

        whitelist_entry = HotkeyWhitelist.objects.filter(ss58_address=hotkey).first()
        if not whitelist_entry:
            raise AuthenticationFailed("Unauthorized hotkey.")

        if whitelist_entry.user is None:
            user, created = User.objects.get_or_create(
                username=hotkey,
                defaults={"is_active": True}
            )
            if created:
                user.set_unusable_password()
                user.save()
            whitelist_entry.user = user
            whitelist_entry.save()
        request.hotkey = hotkey
        return whitelist_entry.user, hotkey
