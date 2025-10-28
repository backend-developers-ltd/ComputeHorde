# FacilitatorClient Redesign

## Problem Statement

The current FacilitatorClient has several critical issues:

1. **No transport abstraction** - Direct WebSocket usage makes testing and alternative transports difficult
2. **Convoluted retry logic** - Nested retries with complex error handling
3. **Poor cancellation handling** - Gets stuck on SIGINTs and doesn't clean up properly
4. **No metrics/observability** - No system events or performance metrics
5. **Concurrent task mess** - Multiple uncoordinated asyncio tasks
6. **Undocumented behavior** - Requires async context manager but doesn't document it
7. **No message ordering** - Lost messages after reconnection break job status updates
8. **Tight coupling** - Handles routing, job processing, and communication in one class

## Design Goals

1. **Separation of Concerns**: FacilitatorClient should only handle communication
2. **Transport Abstraction**: Support different transport mechanisms (WebSocket, HTTP, etc.)
3. **Robust Error Handling**: Proper cancellation, retry logic, and error recovery
4. **Observability**: Comprehensive metrics and system events
5. **Message Ordering**: Preserve message order across reconnections
6. **Testability**: Easy to unit test and mock
7. **Documentation**: Clear API and behavior documentation

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    FacilitatorClient                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   Transport     │  │ Message Queue   │  │   Metrics    │ │
│  │   Manager       │  │   Manager       │  │   Manager    │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   Connection    │  │   Heartbeat     │  │   Job        │ │
│  │   Manager       │  │   Manager       │  │   Dispatcher │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────┐
                    │   Celery Tasks      │
                    │   (Job Processing)  │
                    └─────────────────────┘
```

## Component Design

### 1. Transport Abstraction

```python
from abc import ABC, abstractmethod
from typing import AsyncIterator, Optional
import asyncio
from enum import Enum

class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"

class TransportError(Exception):
    """Base exception for transport-related errors"""
    pass

class ConnectionLostError(TransportError):
    """Connection was lost and needs to be re-established"""
    pass

class AbstractTransport(ABC):
    """Abstract transport interface"""
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to facilitator"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to facilitator"""
        pass
    
    @abstractmethod
    async def send(self, message: str) -> None:
        """Send message to facilitator"""
        pass
    
    @abstractmethod
    async def receive(self) -> str:
        """Receive message from facilitator"""
        pass
    
    @abstractmethod
    async def is_connected(self) -> bool:
        """Check if connection is active"""
        pass
    
    @property
    @abstractmethod
    def state(self) -> ConnectionState:
        """Get current connection state"""
        pass

class WebSocketTransport(AbstractTransport):
    """WebSocket implementation of transport"""
    
    def __init__(self, uri: str, headers: dict = None, **kwargs):
        self.uri = uri
        self.headers = headers or {}
        self._ws: Optional[websockets.ClientConnection] = None
        self._state = ConnectionState.DISCONNECTED
        self._connect_lock = asyncio.Lock()
        
    async def connect(self) -> None:
        async with self._connect_lock:
            if self._state == ConnectionState.CONNECTED:
                return
                
            self._state = ConnectionState.CONNECTING
            try:
                self._ws = await websockets.connect(
                    self.uri, 
                    additional_headers=self.headers,
                    **kwargs
                )
                self._state = ConnectionState.CONNECTED
            except Exception as e:
                self._state = ConnectionState.FAILED
                raise ConnectionLostError(f"Failed to connect: {e}") from e
    
    async def disconnect(self) -> None:
        if self._ws:
            await self._ws.close()
            self._ws = None
        self._state = ConnectionState.DISCONNECTED
    
    async def send(self, message: str) -> None:
        if not await self.is_connected():
            raise ConnectionLostError("Not connected")
        await self._ws.send(message)
    
    async def receive(self) -> str:
        if not await self.is_connected():
            raise ConnectionLostError("Not connected")
        return await self._ws.recv()
    
    async def is_connected(self) -> bool:
        return (self._ws is not None and 
                self._ws.state == websockets.State.OPEN)
    
    @property
    def state(self) -> ConnectionState:
        return self._state
```

### 2. Message Queue Manager

```python
import asyncio
from collections import deque
from typing import Deque, Optional, Callable, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class QueuedMessage:
    """Represents a message queued for sending"""
    content: str
    priority: int = 0  # Higher priority = sent first
    created_at: datetime
    retry_count: int = 0
    max_retries: int = 3
    
    def __lt__(self, other):
        """For priority queue ordering"""
        return self.priority > other.priority

class MessageQueueManager:
    """Manages message queuing and ordering"""
    
    def __init__(self, max_queue_size: int = 1000):
        self._queue: Deque[QueuedMessage] = deque(maxlen=max_queue_size)
        self._queue_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._on_message_sent: Optional[Callable[[str], None]] = None
        self._on_message_failed: Optional[Callable[[str, Exception], None]] = None
    
    async def enqueue_message(
        self, 
        content: str, 
        priority: int = 0,
        max_retries: int = 3
    ) -> None:
        """Add message to queue with ordering preservation"""
        async with self._queue_lock:
            message = QueuedMessage(
                content=content,
                priority=priority,
                created_at=datetime.utcnow(),
                max_retries=max_retries
            )
            self._queue.append(message)
    
    async def get_next_message(self) -> Optional[QueuedMessage]:
        """Get next message to send, maintaining order"""
        async with self._queue_lock:
            if not self._queue:
                return None
            return self._queue.popleft()
    
    async def retry_message(self, message: QueuedMessage) -> None:
        """Put message back in queue for retry"""
        if message.retry_count < message.max_retries:
            message.retry_count += 1
            async with self._queue_lock:
                self._queue.appendleft(message)  # Put back at front
    
    def set_message_sent_callback(self, callback: Callable[[str], None]) -> None:
        """Set callback for when message is successfully sent"""
        self._on_message_sent = callback
    
    def set_message_failed_callback(self, callback: Callable[[str, Exception], None]) -> None:
        """Set callback for when message fails to send"""
        self._on_message_failed = callback
    
    async def send_all_queued(self, transport: AbstractTransport) -> None:
        """Send all queued messages in order"""
        async with self._send_lock:
            while True:
                message = await self.get_next_message()
                if not message:
                    break
                
                try:
                    await transport.send(message.content)
                    if self._on_message_sent:
                        self._on_message_sent(message.content)
                except Exception as e:
                    if self._on_message_failed:
                        self._on_message_failed(message.content, e)
                    await self.retry_message(message)
```

### 3. Connection Manager

```python
import asyncio
import logging
from typing import Optional, Callable
from enum import Enum

class ConnectionManager:
    """Manages connection lifecycle and reconnection logic"""
    
    def __init__(
        self,
        transport: AbstractTransport,
        max_retries: int = 5,
        base_retry_delay: float = 1.0,
        max_retry_delay: float = 60.0,
        jitter: float = 0.1
    ):
        self.transport = transport
        self.max_retries = max_retries
        self.base_retry_delay = base_retry_delay
        self.max_retry_delay = max_retry_delay
        self.jitter = jitter
        self._retry_count = 0
        self._is_running = False
        self._connection_task: Optional[asyncio.Task] = None
        self._on_connected: Optional[Callable] = None
        self._on_disconnected: Optional[Callable] = None
        self._on_connection_failed: Optional[Callable] = None
        
    def set_on_connected(self, callback: Callable) -> None:
        """Set callback for successful connection"""
        self._on_connected = callback
    
    def set_on_disconnected(self, callback: Callable) -> None:
        """Set callback for disconnection"""
        self._on_disconnected = callback
    
    def set_on_connection_failed(self, callback: Callable) -> None:
        """Set callback for connection failure"""
        self._on_connection_failed = callback
    
    async def start(self) -> None:
        """Start connection management"""
        if self._is_running:
            return
        self._is_running = True
        self._connection_task = asyncio.create_task(self._connection_loop())
    
    async def stop(self) -> None:
        """Stop connection management"""
        self._is_running = False
        if self._connection_task:
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                pass
        await self.transport.disconnect()
    
    async def _connection_loop(self) -> None:
        """Main connection loop with retry logic"""
        while self._is_running:
            try:
                await self.transport.connect()
                self._retry_count = 0
                if self._on_connected:
                    await self._on_connected()
                    
            except ConnectionLostError as e:
                logging.warning(f"Connection lost: {e}")
                if self._on_disconnected:
                    await self._on_disconnected()
                
                if not self._is_running:
                    break
                    
                await self._handle_reconnection()
                
            except Exception as e:
                logging.error(f"Unexpected connection error: {e}")
                if self._on_connection_failed:
                    await self._on_connection_failed(e)
                break
    
    async def _handle_reconnection(self) -> None:
        """Handle reconnection with exponential backoff"""
        if self._retry_count >= self.max_retries:
            logging.error(f"Max retries ({self.max_retries}) exceeded")
            if self._on_connection_failed:
                await self._on_connection_failed(
                    Exception(f"Max retries exceeded: {self.max_retries}")
                )
            return
        
        self._retry_count += 1
        delay = min(
            self.base_retry_delay * (2 ** (self._retry_count - 1)),
            self.max_retry_delay
        )
        
        # Add jitter to prevent thundering herd
        jitter_amount = delay * self.jitter
        delay += asyncio.get_running_loop().time() % jitter_amount
        
        logging.info(f"Reconnecting in {delay:.2f}s (attempt {self._retry_count})")
        await asyncio.sleep(delay)
    
    async def ensure_connected(self) -> bool:
        """Ensure connection is active, return True if connected"""
        if await self.transport.is_connected():
            return True
        
        try:
            await self.transport.connect()
            return True
        except Exception as e:
            logging.warning(f"Failed to ensure connection: {e}")
            return False
```

### 4. Metrics Manager

```python
import time
from typing import Dict, Any
from dataclasses import dataclass, field
from collections import defaultdict

@dataclass
class ConnectionMetrics:
    """Connection-related metrics"""
    total_connections: int = 0
    successful_connections: int = 0
    failed_connections: int = 0
    reconnections: int = 0
    last_connection_time: Optional[float] = None
    connection_duration: float = 0.0
    
@dataclass
class MessageMetrics:
    """Message-related metrics"""
    messages_sent: int = 0
    messages_received: int = 0
    messages_failed: int = 0
    messages_queued: int = 0
    messages_retried: int = 0
    last_sent_time: Optional[float] = None
    last_received_time: Optional[float] = None

@dataclass
class JobMetrics:
    """Job-related metrics"""
    jobs_received: int = 0
    jobs_dispatched: int = 0
    jobs_failed: int = 0
    jobs_by_type: Dict[str, int] = field(default_factory=dict)

class MetricsManager:
    """Manages metrics collection and reporting"""
    
    def __init__(self):
        self.connection = ConnectionMetrics()
        self.messages = MessageMetrics()
        self.jobs = JobMetrics()
        self._start_time = time.time()
    
    def record_connection_attempt(self) -> None:
        """Record connection attempt"""
        self.connection.total_connections += 1
    
    def record_connection_success(self) -> None:
        """Record successful connection"""
        self.connection.successful_connections += 1
        self.connection.last_connection_time = time.time()
    
    def record_connection_failure(self) -> None:
        """Record connection failure"""
        self.connection.failed_connections += 1
    
    def record_reconnection(self) -> None:
        """Record reconnection"""
        self.connection.reconnections += 1
    
    def record_message_sent(self) -> None:
        """Record message sent"""
        self.messages.messages_sent += 1
        self.messages.last_sent_time = time.time()
    
    def record_message_received(self) -> None:
        """Record message received"""
        self.messages.messages_received += 1
        self.messages.last_received_time = time.time()
    
    def record_message_failed(self) -> None:
        """Record message failure"""
        self.messages.messages_failed += 1
    
    def record_message_queued(self) -> None:
        """Record message queued"""
        self.messages.messages_queued += 1
    
    def record_message_retry(self) -> None:
        """Record message retry"""
        self.messages.messages_retried += 1
    
    def record_job_received(self, job_type: str = "unknown") -> None:
        """Record job received"""
        self.jobs.jobs_received += 1
        self.jobs.jobs_by_type[job_type] = self.jobs.jobs_by_type.get(job_type, 0) + 1
    
    def record_job_dispatched(self) -> None:
        """Record job dispatched"""
        self.jobs.jobs_dispatched += 1
    
    def record_job_failed(self) -> None:
        """Record job failure"""
        self.jobs.jobs_failed += 1
    
    def get_uptime(self) -> float:
        """Get uptime in seconds"""
        return time.time() - self._start_time
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        return {
            "uptime_seconds": self.get_uptime(),
            "connection": {
                "total": self.connection.total_connections,
                "successful": self.connection.successful_connections,
                "failed": self.connection.failed_connections,
                "reconnections": self.connection.reconnections,
                "success_rate": (
                    self.connection.successful_connections / 
                    max(self.connection.total_connections, 1)
                )
            },
            "messages": {
                "sent": self.messages.messages_sent,
                "received": self.messages.messages_received,
                "failed": self.messages.messages_failed,
                "queued": self.messages.messages_queued,
                "retried": self.messages.messages_retried,
                "success_rate": (
                    self.messages.messages_sent / 
                    max(self.messages.messages_sent + self.messages.messages_failed, 1)
                )
            },
            "jobs": {
                "received": self.jobs.jobs_received,
                "dispatched": self.jobs.jobs_dispatched,
                "failed": self.jobs.jobs_failed,
                "by_type": dict(self.jobs.jobs_by_type),
                "dispatch_rate": (
                    self.jobs.jobs_dispatched / 
                    max(self.jobs.jobs_received, 1)
                )
            }
        }
```

### 5. Heartbeat Manager

```python
import asyncio
import logging
from typing import Optional, Callable

class HeartbeatManager:
    """Manages heartbeat messages to facilitator"""
    
    def __init__(self, interval: float = 60.0):
        self.interval = interval
        self._task: Optional[asyncio.Task] = None
        self._is_running = False
        self._on_heartbeat: Optional[Callable[[], None]] = None
    
    def set_heartbeat_callback(self, callback: Callable[[], None]) -> None:
        """Set callback for heartbeat messages"""
        self._on_heartbeat = callback
    
    async def start(self) -> None:
        """Start heartbeat task"""
        if self._is_running:
            return
        self._is_running = True
        self._task = asyncio.create_task(self._heartbeat_loop())
    
    async def stop(self) -> None:
        """Stop heartbeat task"""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def _heartbeat_loop(self) -> None:
        """Main heartbeat loop"""
        while self._is_running:
            try:
                await asyncio.sleep(self.interval)
                if self._is_running and self._on_heartbeat:
                    await self._on_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Heartbeat error: {e}")
```

### 6. Job Dispatcher

```python
import asyncio
import logging
from typing import Dict, Any, Optional, Callable
from celery import Celery

class JobDispatcher:
    """Dispatches job requests to Celery tasks"""
    
    def __init__(self, celery_app: Celery):
        self.celery_app = celery_app
        self._on_job_dispatched: Optional[Callable[[str], None]] = None
        self._on_job_failed: Optional[Callable[[str, Exception], None]] = None
    
    def set_job_dispatched_callback(self, callback: Callable[[str], None]) -> None:
        """Set callback for successful job dispatch"""
        self._on_job_dispatched = callback
    
    def set_job_failed_callback(self, callback: Callable[[str, Exception], None]) -> None:
        """Set callback for failed job dispatch"""
        self._on_job_failed = callback
    
    async def dispatch_job(self, job_request: Dict[str, Any]) -> str:
        """Dispatch job request to Celery task"""
        try:
            # Dispatch to appropriate Celery task based on job type
            task_name = self._get_task_name(job_request)
            task_result = self.celery_app.send_task(
                task_name,
                args=[job_request],
                queue="organic_jobs"
            )
            
            if self._on_job_dispatched:
                await self._on_job_dispatched(job_request.get("uuid", "unknown"))
            
            return task_result.id
            
        except Exception as e:
            if self._on_job_failed:
                await self._on_job_failed(job_request.get("uuid", "unknown"), e)
            raise
    
    def _get_task_name(self, job_request: Dict[str, Any]) -> str:
        """Determine appropriate Celery task name"""
        # This would be configurable based on job type
        return "compute_horde_validator.validator.tasks.execute_organic_job_request"
```

### 7. Redesigned FacilitatorClient

```python
import asyncio
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

from compute_horde.fv_protocol.facilitator_requests import (
    OrganicJobRequest,
    V0AuthenticationRequest,
    V0Heartbeat,
    V0MachineSpecsUpdate,
)
from compute_horde.fv_protocol.validator_requests import JobStatusUpdate
from compute_horde_core.signature import SignedRequest

from .transport import AbstractTransport, WebSocketTransport
from .message_queue import MessageQueueManager
from .connection import ConnectionManager
from .metrics import MetricsManager
from .heartbeat import HeartbeatManager
from .job_dispatcher import JobDispatcher

logger = logging.getLogger(__name__)

@dataclass
class FacilitatorClientConfig:
    """Configuration for FacilitatorClient"""
    facilitator_uri: str
    keypair: Any  # bittensor_wallet.Keypair
    heartbeat_interval: float = 60.0
    max_queue_size: int = 1000
    max_retries: int = 5
    base_retry_delay: float = 1.0
    max_retry_delay: float = 60.0
    jitter: float = 0.1
    additional_headers: Optional[Dict[str, str]] = None

class FacilitatorClient:
    """
    Redesigned FacilitatorClient with proper separation of concerns.
    
    This client acts as a dumb proxy that:
    1. Receives job requests from facilitator
    2. Dispatches them to Celery tasks
    3. Sends status updates back to facilitator
    4. Maintains connection and message ordering
    """
    
    def __init__(self, config: FacilitatorClientConfig, celery_app):
        self.config = config
        self.celery_app = celery_app
        
        # Initialize components
        self.transport = WebSocketTransport(
            uri=config.facilitator_uri,
            headers=config.additional_headers or {}
        )
        
        self.message_queue = MessageQueueManager(
            max_queue_size=config.max_queue_size
        )
        
        self.connection_manager = ConnectionManager(
            transport=self.transport,
            max_retries=config.max_retries,
            base_retry_delay=config.base_retry_delay,
            max_retry_delay=config.max_retry_delay,
            jitter=config.jitter
        )
        
        self.metrics = MetricsManager()
        self.heartbeat_manager = HeartbeatManager(
            interval=config.heartbeat_interval
        )
        self.job_dispatcher = JobDispatcher(celery_app)
        
        # Setup callbacks
        self._setup_callbacks()
        
        # State
        self._is_running = False
        self._main_task: Optional[asyncio.Task] = None
    
    def _setup_callbacks(self) -> None:
        """Setup component callbacks"""
        # Connection callbacks
        self.connection_manager.set_on_connected(self._on_connected)
        self.connection_manager.set_on_disconnected(self._on_disconnected)
        self.connection_manager.set_on_connection_failed(self._on_connection_failed)
        
        # Message callbacks
        self.message_queue.set_message_sent_callback(self._on_message_sent)
        self.message_queue.set_message_failed_callback(self._on_message_failed)
        
        # Heartbeat callback
        self.heartbeat_manager.set_heartbeat_callback(self._send_heartbeat)
        
        # Job dispatcher callbacks
        self.job_dispatcher.set_job_dispatched_callback(self._on_job_dispatched)
        self.job_dispatcher.set_job_failed_callback(self._on_job_failed)
    
    async def start(self) -> None:
        """
        Start the FacilitatorClient.
        
        This method starts all components and begins the main message loop.
        """
        if self._is_running:
            logger.warning("FacilitatorClient is already running")
            return
        
        logger.info("Starting FacilitatorClient")
        self._is_running = True
        
        # Start all components
        await self.connection_manager.start()
        await self.heartbeat_manager.start()
        
        # Start main message loop
        self._main_task = asyncio.create_task(self._main_loop())
        
        logger.info("FacilitatorClient started successfully")
    
    async def stop(self) -> None:
        """
        Stop the FacilitatorClient.
        
        This method gracefully shuts down all components.
        """
        if not self._is_running:
            logger.warning("FacilitatorClient is not running")
            return
        
        logger.info("Stopping FacilitatorClient")
        self._is_running = False
        
        # Cancel main task
        if self._main_task:
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass
        
        # Stop all components
        await self.heartbeat_manager.stop()
        await self.connection_manager.stop()
        
        logger.info("FacilitatorClient stopped")
    
    async def _main_loop(self) -> None:
        """Main message processing loop"""
        try:
            while self._is_running:
                try:
                    # Ensure connection is active
                    if not await self.connection_manager.ensure_connected():
                        await asyncio.sleep(1)
                        continue
                    
                    # Process incoming messages
                    await self._process_incoming_messages()
                    
                    # Send queued messages
                    await self.message_queue.send_all_queued(self.transport)
                    
                    # Small delay to prevent busy waiting
                    await asyncio.sleep(0.01)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    await asyncio.sleep(1)
                    
        except asyncio.CancelledError:
            logger.info("Main loop cancelled")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
            raise
    
    async def _process_incoming_messages(self) -> None:
        """Process incoming messages from facilitator"""
        try:
            # Non-blocking receive
            message = await asyncio.wait_for(
                self.transport.receive(), 
                timeout=0.1
            )
            
            self.metrics.record_message_received()
            await self._handle_message(message)
            
        except asyncio.TimeoutError:
            # No message received, continue
            pass
        except Exception as e:
            logger.error(f"Error processing incoming message: {e}")
            raise
    
    async def _handle_message(self, message: str) -> None:
        """Handle incoming message from facilitator"""
        try:
            # Parse message based on type
            if self._is_job_request(message):
                await self._handle_job_request(message)
            elif self._is_authentication_response(message):
                await self._handle_authentication_response(message)
            else:
                logger.warning(f"Unknown message type: {message[:100]}...")
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            raise
    
    async def _handle_job_request(self, message: str) -> None:
        """Handle job request from facilitator"""
        try:
            # Parse job request
            job_request = OrganicJobRequest.model_validate_json(message)
            
            self.metrics.record_job_received("organic")
            
            # Dispatch to Celery task
            await self.job_dispatcher.dispatch_job(job_request.model_dump())
            
            logger.info(f"Dispatched job {job_request.uuid} to Celery")
            
        except Exception as e:
            logger.error(f"Error handling job request: {e}")
            self.metrics.record_job_failed()
            raise
    
    async def _handle_authentication_response(self, message: str) -> None:
        """Handle authentication response from facilitator"""
        # Implementation depends on specific response format
        logger.info("Received authentication response")
    
    def _is_job_request(self, message: str) -> bool:
        """Check if message is a job request"""
        # Simple heuristic - could be more sophisticated
        return '"uuid"' in message and '"executor_class"' in message
    
    def _is_authentication_response(self, message: str) -> bool:
        """Check if message is authentication response"""
        return '"status"' in message and '"success"' in message
    
    async def send_job_status_update(self, status_update: JobStatusUpdate) -> None:
        """Send job status update to facilitator"""
        try:
            message = status_update.model_dump_json()
            await self.message_queue.enqueue_message(
                content=message,
                priority=1  # Status updates have higher priority
            )
            logger.debug(f"Queued status update for job {status_update.uuid}")
            
        except Exception as e:
            logger.error(f"Error queuing status update: {e}")
            raise
    
    async def send_machine_specs(self, specs: V0MachineSpecsUpdate) -> None:
        """Send machine specs to facilitator"""
        try:
            message = specs.model_dump_json()
            await self.message_queue.enqueue_message(
                content=message,
                priority=0  # Normal priority
            )
            logger.debug("Queued machine specs update")
            
        except Exception as e:
            logger.error(f"Error queuing machine specs: {e}")
            raise
    
    async def _send_heartbeat(self) -> None:
        """Send heartbeat to facilitator"""
        try:
            heartbeat = V0Heartbeat()
            message = heartbeat.model_dump_json()
            await self.message_queue.enqueue_message(
                content=message,
                priority=0
            )
            logger.debug("Queued heartbeat")
            
        except Exception as e:
            logger.error(f"Error queuing heartbeat: {e}")
    
    async def _on_connected(self) -> None:
        """Handle successful connection"""
        logger.info("Connected to facilitator")
        self.metrics.record_connection_success()
        
        # Send authentication request
        auth_request = V0AuthenticationRequest.from_keypair(self.config.keypair)
        await self.message_queue.enqueue_message(
            content=auth_request.model_dump_json(),
            priority=2  # Authentication has highest priority
        )
    
    async def _on_disconnected(self) -> None:
        """Handle disconnection"""
        logger.warning("Disconnected from facilitator")
        self.metrics.record_connection_failure()
    
    async def _on_connection_failed(self, error: Exception) -> None:
        """Handle connection failure"""
        logger.error(f"Connection failed: {error}")
        self.metrics.record_connection_failure()
    
    async def _on_message_sent(self, message: str) -> None:
        """Handle successful message send"""
        self.metrics.record_message_sent()
        logger.debug("Message sent successfully")
    
    async def _on_message_failed(self, message: str, error: Exception) -> None:
        """Handle message send failure"""
        self.metrics.record_message_failed()
        logger.warning(f"Message send failed: {error}")
    
    async def _on_job_dispatched(self, job_uuid: str) -> None:
        """Handle successful job dispatch"""
        self.metrics.record_job_dispatched()
        logger.info(f"Job {job_uuid} dispatched successfully")
    
    async def _on_job_failed(self, job_uuid: str, error: Exception) -> None:
        """Handle job dispatch failure"""
        self.metrics.record_job_failed()
        logger.error(f"Job {job_uuid} dispatch failed: {error}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return self.metrics.get_metrics_summary()
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()
```

## Usage Example

```python
import asyncio
from celery import Celery
from bittensor_wallet import Keypair

# Initialize Celery app
celery_app = Celery('compute_horde_validator')

# Create configuration
config = FacilitatorClientConfig(
    facilitator_uri="wss://facilitator.example.com/ws/v0/",
    keypair=Keypair.from_mnemonic("your mnemonic"),
    heartbeat_interval=60.0,
    max_queue_size=1000,
    max_retries=5
)

# Create and use client
async def main():
    async with FacilitatorClient(config, celery_app) as client:
        # Client automatically starts and manages connection
        # Job requests are automatically dispatched to Celery
        # Status updates are automatically sent back to facilitator
        
        # You can send status updates manually if needed
        status_update = JobStatusUpdate(
            uuid="job-123",
            status=JobStatus.COMPLETED
        )
        await client.send_job_status_update(status_update)
        
        # Get metrics
        metrics = client.get_metrics()
        print(f"Uptime: {metrics['uptime_seconds']}s")
        print(f"Jobs dispatched: {metrics['jobs']['dispatched']}")

# Run the client
asyncio.run(main())
```

## Migration Strategy

### Phase 1: Create New Components
1. Implement transport abstraction
2. Implement message queue manager
3. Implement connection manager
4. Implement metrics manager
5. Implement heartbeat manager
6. Implement job dispatcher

### Phase 2: Create New FacilitatorClient
1. Implement redesigned FacilitatorClient
2. Add comprehensive tests
3. Add documentation

### Phase 3: Gradual Migration
1. Deploy new client alongside old one
2. A/B test with subset of validators
3. Monitor metrics and performance
4. Gradually migrate all validators

### Phase 4: Cleanup
1. Remove old FacilitatorClient
2. Remove unused code
3. Update documentation

## Benefits of New Design

1. **Separation of Concerns**: Each component has a single responsibility
2. **Testability**: Easy to unit test individual components
3. **Observability**: Comprehensive metrics and logging
4. **Reliability**: Proper error handling and recovery
5. **Maintainability**: Clear interfaces and documentation
6. **Extensibility**: Easy to add new transport types or features
7. **Performance**: Efficient message queuing and processing
8. **Message Ordering**: Preserves message order across reconnections

## Testing Strategy

### Unit Tests
- Test each component in isolation
- Mock dependencies
- Test error conditions
- Test edge cases

### Integration Tests
- Test component interactions
- Test with real transport
- Test message flow
- Test error recovery

### End-to-End Tests
- Test with real facilitator
- Test job dispatch flow
- Test metrics collection
- Test performance under load

This redesigned FacilitatorClient addresses all the identified issues while providing a clean, maintainable, and extensible architecture.
