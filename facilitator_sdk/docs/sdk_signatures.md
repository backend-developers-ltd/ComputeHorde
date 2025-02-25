# Signatures

## Client side

If `signer` parameter is provided to `FacilitatorClient`,
the client will sign every request before sending it to the server.

## Verifying signatures on server side

```python
import json

from compute_horde.signature import verify_request

signature = verify_request(
    request.method,
    str(request.url),
    dict(request.headers),
    json=json.loads(request.content) if request.content else None,
)
```


Additionally, in case of `bittensor` signature type, you can extract hotkey from the signature.
```python
if signature and signature.signature_type == "bittensor":
    hotkey = signature.signatory
```
