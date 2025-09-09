from aiohttp import ClientResponse
from compute_horde_core.executor_class import ExecutorClass


async def parse_manifest_response(response: ClientResponse) -> dict[ExecutorClass, int]:
    """
    Parse the manifest response and validate the format.
    """
    try:
        json_response = await response.json()
    except Exception:
        raise ValueError(f"Manifest response is not valid JSON: {response}")

    try:
        manifest: dict[str, int] = json_response["manifest"]
    except KeyError:
        raise ValueError(f"Manifest response body is missing 'manifest' key: {manifest}")

    # Validate manifest format
    try:
        manifest = {ExecutorClass(key): int(value) for key, value in manifest.items()}
    except Exception:
        raise ValueError(f"Manifest has invalid format: {manifest}")

    if len(manifest) == 0:
        raise ValueError(f"Manifest is empty: {manifest}")

    return manifest
