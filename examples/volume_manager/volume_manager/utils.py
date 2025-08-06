import logging

from pydantic import TypeAdapter

from .schemas import VolumeSpec

from compute_horde_core.volume import Volume

logger = logging.getLogger(__name__)


def create_volume_from_spec(volume_spec: VolumeSpec) -> "Volume":
    """Create a Volume object from VolumeSpec."""
    try:
        volume_data = volume_spec.model_dump()
        logger.info(f"Creating {volume_spec.volume_type} volume")

        volume_adapter = TypeAdapter(Volume)
        return volume_adapter.validate_python(volume_data)

    except Exception as e:
        logger.exception(f"Failed to create {volume_spec.volume_type} volume")
        raise ValueError(f"Failed to create {volume_spec.volume_type} volume: {e}")
