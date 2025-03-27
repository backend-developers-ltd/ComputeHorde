import secrets
import time

import jwt
from bittensor import Keypair
from django.conf import settings
from django.core.signing import BadSignature, SignatureExpired, TimestampSigner
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .models import HotkeyWhitelist


@api_view(["GET"])
def auth_nonce_view(request):
    signer = TimestampSigner()
    nonce = secrets.token_hex(16)
    signed_nonce = signer.sign(nonce)
    return Response({"nonce": signed_nonce})


@api_view(["POST"])
def auth_login_view(request):
    hotkey = request.data.get("hotkey")
    signature = request.data.get("signature")
    signed_nonce = request.data.get("nonce")
    if not hotkey:
        return Response({"error": "Missing hotkey"}, status=status.HTTP_400_BAD_REQUEST)
    if not signature:
        return Response({"error": "Missing signature"}, status=status.HTTP_400_BAD_REQUEST)
    if not signed_nonce:
        return Response({"error": "Missing nonce"}, status=status.HTTP_400_BAD_REQUEST)

    signer = TimestampSigner()
    nonce_lifetime = settings.NONCE_LIFETIME
    try:
        signer.unsign(signed_nonce, max_age=nonce_lifetime)
    except BadSignature:
        return Response({"error": "Invalid nonce"}, status=status.HTTP_400_BAD_REQUEST)
    except SignatureExpired:
        return Response({"error": "Nonce expired"}, status=status.HTTP_400_BAD_REQUEST)

    try:
        is_valid = Keypair(ss58_address=hotkey).verify(data=signed_nonce.encode(), signature=bytes.fromhex(signature))
    except Exception:
        return Response({"error": "Nonce expired or not found"}, status=status.HTTP_400_BAD_REQUEST)

    if not is_valid:
        return Response({"error": "Invalid signature"}, status=status.HTTP_401_UNAUTHORIZED)

    if not HotkeyWhitelist.objects.filter(ss58_address=hotkey).exists():
        return Response({"error": "Hotkey not allowed"}, status=status.HTTP_400_BAD_REQUEST)

    jwt_lifetime = settings.JWT_LIFETIME
    payload = {
        "sub": hotkey,
        "iat": int(time.time()),
        "exp": int(time.time()) + jwt_lifetime,
    }
    token = jwt.encode(payload, settings.SECRET_KEY, algorithm="HS256")

    return Response({"token": token})
