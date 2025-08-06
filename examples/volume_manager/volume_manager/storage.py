import logging
import os
import pathlib
import shutil

from compute_horde_core.volume import (
    Volume,
    VolumeDownloader,
    VolumeDownloadFailed,
    VolumeType,
)

logger = logging.getLogger(__name__)


class VolumeStorage:
    """Manages volume storage and caching."""

    def __init__(self, cache_dir: str = "/tmp/volume_manager/cache"):
        self.cache_dir = pathlib.Path(cache_dir)
        # Create the cache directory and all parent directories
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create cache directory {self.cache_dir}: {e}")
            raise
        logger.info(f"Volume storage initialized at {self.cache_dir}")

    def _get_volume_path(self, volume_url: str) -> pathlib.Path:
        """Get the path where a volume should be stored."""
        return pathlib.Path(self.cache_dir / volume_url)

    def _is_volume_cached(self, volume_url: str) -> tuple[bool, pathlib.Path]:
        """
        Check if a volume is cached and return its path.

        Args:
            volume_hash: Hash of the volume to check

        Returns:
            Tuple of (is_cached, volume_path)
        """
        volume_path = self._get_volume_path(volume_url)
        is_cached = volume_path.exists() and volume_path.is_dir()
        return is_cached, volume_path

    async def prepare_volume(self, volume: Volume) -> pathlib.Path:
        """
        Prepare a volume by downloading it if not already cached.

        Args:
            volume: The volume to prepare

        Returns:
            Path to the prepared volume

        Raises:
            VolumeDownloadFailed: If download fails
        """
        # Generate a unique identifier for caching based on volume type
        if volume.volume_type == VolumeType.single_file:
            volume_url = volume.url.replace("/", "_").replace(":", "_")
        elif volume.volume_type == VolumeType.huggingface_volume:
            volume_url = f"hf-{volume.repo_id.replace('/', '_')}"
        elif volume.volume_type == VolumeType.zip_url:
            volume_url = volume.contents.replace("/", "_").replace(":", "_")
        elif volume.volume_type == VolumeType.inline:
            if volume.relative_path:
                volume_url = f"inline-{volume.relative_path}"
            else:
                volume_url = f"inline-{len(volume.contents)}"
        else:
            volume_url = f"unknown-{volume.volume_type}"

        is_cached, volume_path = self._is_volume_cached(volume_url)

        # Check if already cached
        if is_cached:
            logger.info(f"Volume {volume_url} found in cache - skipping download")
            return volume_path

        # Download the volume
        logger.info(f"Volume {volume_url} not found in cache - downloading")

        try:
            downloader = VolumeDownloader.for_volume(volume)
            await downloader.download(volume_path)

            logger.info(f"Successfully downloaded volume {volume_url}")
            return volume_path

        except VolumeDownloadFailed as e:
            logger.error(f"Failed to download volume {volume_url}: {e}")
            shutil.rmtree(volume_path, ignore_errors=True)
            raise
        except Exception:
            logger.exception(f"Unexpected error during download of {volume_url}")
            if volume_path.exists():
                shutil.rmtree(volume_path, ignore_errors=True)
            raise

    def cleanup_volume(self, volume_url: str) -> bool:
        """
        Clean up a volume from cache.

        Args:
            volume_url: URL of the volume to clean up

        Returns:
            True if cleanup was successful, False if volume didn't exist
        """
        volume_path = self._get_volume_path(volume_url)

        if volume_path.exists():
            try:
                shutil.rmtree(volume_path, ignore_errors=True)
                logger.info(f"Cleaned up volume {volume_url}")
                return True
            except Exception:
                logger.exception(f"Failed to clean up volume {volume_url}")
                return False
        else:
            return False

    def list_cached_volumes(self) -> list[str]:
        """List all cached volume URLs."""
        if not self.cache_dir.exists():
            return []

        volumes = []
        for item in self.cache_dir.iterdir():
            if item.is_dir():
                volumes.append(item.name)

        return volumes

    def get_cache_size(self) -> int:
        """Get the total size of the cache in bytes."""
        if not self.cache_dir.exists():
            return 0

        total_size = 0
        for root, _, files in os.walk(self.cache_dir):
            for file in files:
                file_path = pathlib.Path(root) / file
                if file_path.exists():
                    total_size += file_path.stat().st_size

        return total_size
