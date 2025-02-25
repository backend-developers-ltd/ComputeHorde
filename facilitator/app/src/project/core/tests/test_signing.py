def test__signing__low_level(keypair):
    payload = keypair.public_key
    signature = f"0x{keypair.sign(payload).hex()}"
    assert keypair.verify(payload, signature)


def test__signing__authentication_request__success(authentication_request):
    assert authentication_request.verify_signature()


def test__signing__authentication_request__failure(keypair, authentication_request, other_signature):
    authentication_request.signature = other_signature
    assert not authentication_request.verify_signature()
