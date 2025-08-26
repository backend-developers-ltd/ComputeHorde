from django.conf import settings
from pylon_client import Hotkey, Neuron, PylonClient

_pylon_client_instance: PylonClient | None = None


def pylon_client() -> PylonClient:
    global _pylon_client_instance
    if _pylon_client_instance is None:
        _pylon_client_instance = PylonClient(
            base_url=f"http://{settings.PYLON_ADDRESS}:{settings.PYLON_PORT}",
            timeout=20.0,
        )
    return _pylon_client_instance


def get_serving_hotkeys(neurons: list[Neuron]) -> list[Hotkey]:
    """
    Get serving hotkeys for a list of neurons.

    Args:
        neurons (list[Neuron]): List of Neuron objects.

    Returns:
        list[Hotkey]: List of Hotkey objects.
    """
    return [
        neuron.hotkey for neuron in neurons if neuron.axon_info and neuron.axon_info.ip != "0.0.0.0"
    ]
