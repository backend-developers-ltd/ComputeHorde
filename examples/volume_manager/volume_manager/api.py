import logging
from typing import Any, Dict, List
from fastapi import FastAPI, HTTPException

from .schemas import (
    JobFinishedRequest,
    JobFinishedResponse,
    PrepareVolumeRequest,
    PrepareVolumeResponse,
)
from .utils import create_volume_from_spec
from .storage import VolumeStorage

logger = logging.getLogger(__name__)

# Global storage instance
storage = VolumeStorage()

app = FastAPI(
    title="Volume Manager Example",
    description="Example implementation of a volume manager for Compute Horde",
    version="0.1.0",
)


@app.get("/")
async def root() -> Dict[str, List[str]]:
    """List available endpoints."""
    return {
        "endpoints": [
            "GET / - This directory",
            "GET /health - Health check",
            "GET /cache/status - Cache information",
            "DELETE /cache/clear - Clear cache",
            "POST /prepare_volume - Prepare volume for job",
            "POST /job_finished - Job completion notification"
        ]
    }


@app.post("/prepare_volume", response_model=PrepareVolumeResponse)
async def prepare_volume(request: PrepareVolumeRequest) -> PrepareVolumeResponse:
    """
    Prepare a volume for a job.
    
    This endpoint receives a volume specification and job metadata,
    downloads the volume if not already cached, and returns Docker
    mount options for the volume.
    """

    logger.info(f"Prepare volume request received for job {request.job_uuid}")
    logger.debug(f"Volume type: {request.volume.volume_type}")
    
    try:
        mount_options = []
        
        if request.volume.volume_type == "multi_volume":
            # Handle multi-volume by preparing each sub-volume
            logger.info(f"Processing multi-volume with {len(request.volume.volumes)} sub-volumes")
            for i, sub_volume in enumerate(request.volume.volumes):
                logger.info(f"Processing sub-volume {i}: {sub_volume.model_dump()}")
                volume = create_volume_from_spec(sub_volume)
                volume_path = await storage.prepare_volume(volume)
                
                # Mount each sub-volume to a different path
                mount_path = ("/volume/" + sub_volume.relative_path) if sub_volume.relative_path else "/volume"
                mount_options.append(["-v", f"{volume_path}:{mount_path}"])
                
        else:
            # Handle individual volume
            logger.info(f"Processing individual volume")
            volume = create_volume_from_spec(request.volume)
            volume_path = await storage.prepare_volume(volume)
            
            # Mount to the default volume path
            mount_path = ("/volume/" + request.volume.relative_path) if request.volume.relative_path else "/volume"
            mount_options = [["-v", f"{volume_path}:{mount_path}"]]
        
        logger.info(f"Volume prepared for job {request.job_uuid}")
        logger.debug(f"Returning mount options: {mount_options}")
        
        return PrepareVolumeResponse(mounts=mount_options)
        
    except ValueError as e:
        logger.error(f"Invalid volume specification: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Failed to prepare volume for job {request.job_uuid}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/job_finished", response_model=JobFinishedResponse)
async def job_finished(request: JobFinishedRequest) -> JobFinishedResponse:
    """
    Notify that a job has finished.
    
    This endpoint is called when a job completes. The volume manager
    can use this to clean up resources or update usage statistics.
    """
    try:
        logger.info(f"Job {request.job_uuid} finished")
        
        # For now, we don't clean up volumes immediately
        # In a real implementation, you might want to:
        # - Track volume usage
        # - Implement LRU cache eviction
        # - Clean up unused volumes
        
        return JobFinishedResponse(success=True)
        
    except Exception as e:
        logger.exception(f"Failed to process job finished notification for {request.job_uuid}")
        return JobFinishedResponse(success=False)


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/cache/status")
async def cache_status() -> Dict[str, Any]:
    """Get cache status information."""
    cached_volumes = storage.list_cached_volumes()
    cache_size = storage.get_cache_size()
    
    return {
        "cached_volumes_count": len(cached_volumes),
        "cache_size": f"{cache_size // 1024} KB",
        "cached_volumes": cached_volumes,
    }


@app.delete("/cache/clear")
async def clear_cache() -> Dict[str, str]:
    """Clear all cached volumes."""
    try:
        cached_volumes = storage.list_cached_volumes()
        if not cached_volumes:
            return {"message": "No volumes in cache to clear"}
        
        cleared_count = 0
        for volume_hash in cached_volumes:
            if storage.cleanup_volume(volume_hash):
                cleared_count += 1
        
        logger.info(f"Cleared {cleared_count} volumes from cache")
        return {"message": f"Cleared {cleared_count} volumes from cache"}
        
    except Exception as e:
        logger.exception("Failed to clear cache")
        raise HTTPException(status_code=500, detail=str(e))