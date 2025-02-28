import json
import time

from bittensor import Keypair
from django.conf import settings
from django.contrib.auth.models import AbstractBaseUser, AnonymousUser
from django.http import HttpRequest
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed
from structlog import get_logger

from .models import Miner, Validator

log = get_logger(__name__)


class HotkeyAuthentication(BaseAuthentication):
    def authenticate(self, request: HttpRequest) -> tuple[AbstractBaseUser, str] | None:
        method = request.method.upper()
        hotkey = request.headers.get("Hotkey")
        nonce = request.headers.get("Nonce")
        signature = request.headers.get("Signature")
        if not hotkey and not nonce and not signature:
            return None

        if not all((hotkey, nonce, signature)):
            raise AuthenticationFailed("Missing some of required headers: hotkey, nonce, signature")

        if abs(time.time() - float(nonce)) > int(settings.SIGNATURE_EXPIRE_DURATION):
            raise AuthenticationFailed("Invalid nonce")

        if not (
            Validator.objects.filter(ss58_address=hotkey, is_active=True).exists()
            or Miner.objects.filter(ss58_address=hotkey, is_active=True).exists()
        ):
            raise AuthenticationFailed("Unauthorized hotkey")

        client_headers = {
            "Nonce": nonce,
            "Hotkey": hotkey,
            "Note": request.headers.get("Note"),
            "SubnetID": request.headers.get("SubnetID"),
            "Realm": request.headers.get("Realm"),
        }
        client_headers = {k: v for k, v in client_headers.items() if v is not None}
        headers_str = json.dumps(client_headers, sort_keys=True)

        url = request.build_absolute_uri()
        data_to_sign = f"{method}{url}{headers_str}"

        if "file" in request.FILES:
            uploaded_file = request.FILES["file"]
            file_content = uploaded_file.read()
            decoded_file_content = file_content.decode(errors="ignore")
            data_to_sign += decoded_file_content

        try:
            is_valid = Keypair(ss58_address=hotkey).verify(
                data=data_to_sign.encode(), signature=bytes.fromhex(signature)
            )
        except Exception as exc:
            log.warning("Signature verification failed", exc=exc)
            raise AuthenticationFailed("Signature verification failed") from exc

        if not is_valid:
            raise AuthenticationFailed("Invalid signature.")

        request.hotkey = hotkey
        return AnonymousUser(), hotkey
