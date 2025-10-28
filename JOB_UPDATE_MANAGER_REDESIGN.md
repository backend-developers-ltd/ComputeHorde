# Job Update Manager Redesign: Single Manager + Celery Tasks

## Corrected Understanding: Single Manager Orchestrating Celery Tasks

You're absolutely right! The workflow should be:

1. **Single JobUpdateManager** started by FacilitatorClient
2. **Per-job Celery tasks** that listen for updates (like current `handle_job_status_updates`)
3. **Manager orchestrates** the lifecycle of these Celery tasks

This is much cleaner than my previous misunderstanding. Let me redesign this properly.

## Current Implementation Analysis

### **Current `handle_job_status_updates`:**

```python
async def handle_job_status_updates(self, job_uuid: str):
    """
    Relay job status updates for given job back to the Facilitator.
    Loop until a terminal status is received.
    """
    logger.debug(f"Listening for job status updates for job {job_uuid}")
    try:
        while True:
            msg = await get_channel_layer().receive(f"job_status_updates__{job_uuid}")
            try:
                envelope = _JobStatusChannelEnvelope.model_validate(msg)
                logger.debug(
                    f"Received job status update for job {job_uuid}: status={envelope.payload.status}"
                )
                task = asyncio.create_task(self.send_job_status_update(envelope.payload))
                await self.tasks_to_reap.put(task)
                if not envelope.payload.status.is_in_progress():
                    return
            except pydantic.ValidationError as exc:
                logger.warning("Received malformed job status update: %s")
        except Exception as e:
            # Nothing that gets thrown here is expected.
            sentry_sdk.capture_exception(e)
            logger.warning("Error in job status update listener", exc_info=True)
        finally:
            logger.debug(f"Finished listening for job status updates for job {job_uuid}")
```

**Key Characteristics:**
- **Per-job listener**: Each job gets its own listener
- **Channel-based**: Uses Django Channels for job status updates
- **Terminal status**: Stops listening when job reaches terminal status
- **Task management**: Creates asyncio tasks for sending status updates
- **Error handling**: Comprehensive error handling and logging

## Redesigned Approach: Single Manager + Celery Tasks

### **1. JobUpdateManager: Orchestrates Celery Tasks**

```python
import asyncio
import logging
from typing import Dict, Set, Optional, Callable
from dataclasses import dataclass
from datetime import datetime

@dataclass
class JobListener:
    """Represents a job listener task"""
    job_uuid: str
    celery_task_id: str
    created_at: datetime
    is_active: bool = True

class JobUpdateManager:
    """Manages job status update listeners using Celery tasks"""
    
    def __init__(self, celery_app, message_queue: MessageQueueManager):
        self.celery_app = celery_app
        self.message_queue = message_queue
        self._listeners: Dict[str, JobListener] = {}
        self._is_running = False
        self._on_job_completed: Optional[Callable] = None
        
    def set_on_job_completed(self, callback: Callable) -> None:
        """Set callback for job completion"""
        self._on_job_completed = callback
    
    async def start(self) -> None:
        """Start the job update manager"""
        self._is_running = True
        logging.info("Job update manager started")
    
    async def stop(self) -> None:
        """Stop the job update manager"""
        self._is_running = False
        
        # Cancel all active listeners
        for job_uuid, listener in list(self._listeners.items()):
            await self.stop_listening_for_job(job_uuid)
        
        logging.info("Job update manager stopped")
    
    async def start_listening_for_job(self, job_uuid: str) -> None:
        """Start listening for status updates for a specific job using Celery task"""
        if job_uuid in self._listeners:
            logging.warning(f"Already listening for job {job_uuid}")
            return
        
        try:
            # Start Celery task for this job
            task = listen_for_job_status_updates.delay(job_uuid)
            
            listener = JobListener(
                job_uuid=job_uuid,
                celery_task_id=task.id,
                created_at=datetime.utcnow()
            )
            
            self._listeners[job_uuid] = listener
            
            logging.info(f"Started Celery task for job status updates: {job_uuid} (task_id: {task.id})")
            
        except Exception as e:
            logging.error(f"Error starting job status listener for {job_uuid}: {e}")
            raise
    
    async def stop_listening_for_job(self, job_uuid: str) -> None:
        """Stop listening for status updates for a specific job"""
        if job_uuid not in self._listeners:
            logging.warning(f"Not listening for job {job_uuid}")
            return
        
        listener = self._listeners[job_uuid]
        
        try:
            # Revoke Celery task
            self.celery_app.control.revoke(listener.celery_task_id, terminate=True)
            
            # Remove listener
            del self._listeners[job_uuid]
            
            logging.info(f"Stopped listening for job status updates: {job_uuid}")
            
        except Exception as e:
            logging.error(f"Error stopping job status listener for {job_uuid}: {e}")
            raise
    
    async def handle_job_completion(self, job_uuid: str) -> None:
        """Handle job completion"""
        try:
            # Stop listening for this job
            await self.stop_listening_for_job(job_uuid)
            
            if self._on_job_completed:
                await self._on_job_completed(job_uuid)
            
            logging.info(f"Job {job_uuid} completed, stopped listening for updates")
            
        except Exception as e:
            logging.error(f"Error handling job completion for {job_uuid}: {e}")
            raise
    
    def get_active_jobs(self) -> Set[str]:
        """Get set of active job UUIDs"""
        return set(self._listeners.keys())
    
    def get_listener_status(self, job_uuid: str) -> Optional[dict]:
        """Get status of listener for specific job"""
        if job_uuid not in self._listeners:
            return None
        
        listener = self._listeners[job_uuid]
        return {
            "job_uuid": job_uuid,
            "celery_task_id": listener.celery_task_id,
            "created_at": listener.created_at,
            "is_active": listener.is_active
        }
```

### **2. Celery Task: Per-Job Status Update Listener**

```python
from celery import shared_task
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from pydantic import ValidationError
import logging

@shared_task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 60})
def listen_for_job_status_updates(self, job_uuid: str) -> None:
    """
    Celery task that listens for job status updates for a specific job.
    This replaces the current handle_job_status_updates method.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting job status update listener for job {job_uuid}")
    
    try:
        # Run the async listener
        async_to_sync(_listen_for_job_status_async)(job_uuid, self.request.id)
        
    except Exception as e:
        logger.error(f"Error in job status update listener for {job_uuid}: {e}")
        raise

async def _listen_for_job_status_async(job_uuid: str, task_id: str) -> None:
    """
    Async function that listens for job status updates.
    This is the core logic from the current handle_job_status_updates method.
    """
    logger = logging.getLogger(__name__)
    channel_layer = get_channel_layer()
    
    logger.debug(f"Listening for job status updates for job {job_uuid}")
    
    try:
        while True:
            # Receive message from channel (same as current implementation)
            msg = await channel_layer.receive(f"job_status_updates__{job_uuid}")
            
            try:
                # Parse status update (same as current implementation)
                envelope = _JobStatusChannelEnvelope.model_validate(msg)
                logger.debug(
                    f"Received job status update for job {job_uuid}: status={envelope.payload.status}"
                )
                
                # Send status update to facilitator (same as current implementation)
                await _send_job_status_update(envelope.payload)
                
                # Check if job is terminal (same as current implementation)
                if not envelope.payload.status.is_in_progress():
                    logger.info(f"Job {job_uuid} reached terminal status: {envelope.payload.status}")
                    return
                    
            except ValidationError as exc:
                logger.warning("Received malformed job status update: %s", exc)
                continue
                
    except Exception as e:
        logger.error(f"Error in job status update listener for {job_uuid}: {e}")
        raise
    finally:
        logger.debug(f"Finished listening for job status updates for job {job_uuid}")

async def _send_job_status_update(status_update: JobStatusUpdate) -> None:
    """
    Send job status update to facilitator.
    This replaces the current send_job_status_update method.
    """
    try:
        # Get facilitator client and send via message queue
        facilitator_client = get_facilitator_client()
        if facilitator_client:
            await facilitator_client.message_queue.enqueue_message(
                content=status_update.model_dump_json(),
                priority=1,  # High priority for status updates
                max_retries=3
            )
            
            logger.debug(f"Queued status update for sending: {status_update.uuid}")
        else:
            logger.warning("Facilitator client not available, cannot send status update")
            
    except Exception as e:
        logger.error(f"Error sending job status update: {e}")
        raise

def get_facilitator_client():
    """Get the current facilitator client instance"""
    # This would need to be implemented based on your application structure
    # Could use a singleton pattern or dependency injection
    pass
```

### **3. FacilitatorClient Integration**

```python
class FacilitatorClient:
    """Redesigned FacilitatorClient with job update manager"""
    
    def __init__(self, config, celery_app):
        # ... other initialization ...
        self.job_update_manager = JobUpdateManager(
            celery_app=celery_app,
            message_queue=self.message_queue
        )
        self._is_running = False
        self._main_task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """Start the FacilitatorClient"""
        if self._is_running:
            return
        
        self._is_running = True
        
        # Start components
        await self.connection_manager.start()
        await self.heartbeat_manager.start()
        await self.job_update_manager.start()
        
        # Start main loop
        self._main_task = asyncio.create_task(self._main_loop())
        
        logging.info("FacilitatorClient started")
    
    async def stop(self) -> None:
        """Stop the FacilitatorClient"""
        self._is_running = False
        
        # Stop main loop
        if self._main_task:
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass
        
        # Stop components
        await self.job_update_manager.stop()
        await self.heartbeat_manager.stop()
        await self.connection_manager.stop()
        
        logging.info("FacilitatorClient stopped")
    
    async def _handle_job_request(self, message: str) -> None:
        """Handle job request from facilitator"""
        try:
            # Parse job request
            job_request = OrganicJobRequest.model_validate_json(message)
            
            self.metrics.record_job_received("organic")
            
            # Start listening for job status updates using Celery task
            await self.job_update_manager.start_listening_for_job(job_request.uuid)
            
            # Dispatch to Celery task for job execution
            await self.job_dispatcher.dispatch_job(job_request.model_dump())
            
            logging.info(f"Dispatched job {job_request.uuid} to Celery")
            
        except Exception as e:
            logging.error(f"Error handling job request: {e}")
            self.metrics.record_job_failed()
            raise
    
    async def _handle_job_completion(self, job_uuid: str) -> None:
        """Handle job completion"""
        try:
            # Notify job update manager of completion
            await self.job_update_manager.handle_job_completion(job_uuid)
            
            logging.info(f"Job {job_uuid} completed")
            
        except Exception as e:
            logging.error(f"Error handling job completion for {job_uuid}: {e}")
            raise
```

## Benefits of This Approach

### **1. Clean Separation of Concerns**

| Component | Responsibility |
|-----------|---------------|
| **JobUpdateManager** | Orchestrates Celery tasks for job listeners |
| **Celery Tasks** | Listen for job status updates (per-job) |
| **FacilitatorClient** | Coordinates job lifecycle |

### **2. Scalable Architecture**

- **Per-job tasks**: Each job gets its own Celery task
- **Independent scaling**: Celery workers can be scaled independently
- **Load balancing**: Celery distributes tasks across workers
- **Resource isolation**: Each job listener runs in its own worker

### **3. Reliable Processing**

- **Persistent tasks**: Celery tasks survive process restarts
- **Automatic retry**: Built-in retry mechanism for failed tasks
- **Error handling**: Comprehensive error handling and logging
- **Monitoring**: Built-in monitoring and metrics

### **4. Easy Management**

- **Single manager**: One component manages all job listeners
- **Clean lifecycle**: Start/stop listeners for specific jobs
- **Status tracking**: Track active listeners and their status
- **Easy testing**: Each component can be tested independently

## Workflow Summary

### **1. FacilitatorClient Startup:**
```python
# Start job update manager
await self.job_update_manager.start()
```

### **2. New Job Submission:**
```python
# Start Celery task for job status updates
await self.job_update_manager.start_listening_for_job(job_uuid)
```

### **3. Job Completion:**
```python
# Stop listening for completed job
await self.job_update_manager.handle_job_completion(job_uuid)
```

### **4. FacilitatorClient Shutdown:**
```python
# Stop all job listeners
await self.job_update_manager.stop()
```

## Key Advantages

### **1. Maintains Current Logic**
- **Same channel listening**: Uses existing Django Channels
- **Same status processing**: Identical status update handling
- **Same error handling**: Preserves current error handling logic

### **2. Improves Architecture**
- **Celery-based**: Leverages Celery for reliability and scaling
- **Clean separation**: Manager orchestrates, tasks execute
- **Easy management**: Single point of control for all job listeners

### **3. Enhances Reliability**
- **Persistent tasks**: Tasks survive process restarts
- **Automatic retry**: Built-in retry mechanism
- **Better monitoring**: Celery provides monitoring and metrics

### **4. Simplifies Deployment**
- **Independent scaling**: Job listeners can be scaled independently
- **Resource isolation**: Each job listener runs in its own worker
- **Easy debugging**: Clear separation between orchestration and execution

## Conclusion

**This approach is much cleaner** because it:

1. **Maintains current logic**: Preserves the existing `handle_job_status_updates` logic
2. **Improves architecture**: Uses Celery for reliability and scaling
3. **Clean separation**: Manager orchestrates, tasks execute
4. **Easy management**: Single point of control for all job listeners
5. **Better reliability**: Persistent tasks with automatic retry

The key insight is that **the JobUpdateManager orchestrates Celery tasks** rather than doing the listening itself, which provides the best of both worlds: clean orchestration and reliable execution.
