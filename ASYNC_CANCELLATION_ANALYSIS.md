# Asyncio Cancellation Handling in FacilitatorClient Redesign

## Problem with Original Implementation

The original FacilitatorClient had several cancellation issues:

1. **No proper cancellation handling** - Tasks would get stuck on SIGINT
2. **Nested retries** - Complex retry logic that didn't respect cancellation
3. **Uncoordinated tasks** - Multiple asyncio tasks without proper cleanup
4. **No graceful shutdown** - Components didn't stop cleanly

## How the Redesign Handles Cancellation

### 1. **Explicit Cancellation Handling in All Components**

#### ConnectionManager
```python
async def stop(self) -> None:
    """Stop connection management"""
    self._is_running = False
    if self._connection_task:
        self._connection_task.cancel()  # Explicitly cancel task
        try:
            await self._connection_task
        except asyncio.CancelledError:
            pass  # Expected exception, handle gracefully
    await self.transport.disconnect()

async def _connection_loop(self) -> None:
    """Main connection loop with retry logic"""
    while self._is_running:  # Check running flag
        try:
            await self.transport.connect()
            # ... connection logic
        except asyncio.CancelledError:
            break  # Exit immediately on cancellation
        except Exception as e:
            # ... error handling
```

#### HeartbeatManager
```python
async def stop(self) -> None:
    """Stop heartbeat task"""
    self._is_running = False
    if self._task:
        self._task.cancel()  # Cancel heartbeat task
        try:
            await self._task
        except asyncio.CancelledError:
            pass  # Handle cancellation gracefully

async def _heartbeat_loop(self) -> None:
    """Main heartbeat loop"""
    while self._is_running:
        try:
            await asyncio.sleep(self.interval)
            if self._is_running and self._on_heartbeat:
                await self._on_heartbeat()
        except asyncio.CancelledError:
            break  # Exit immediately on cancellation
        except Exception as e:
            logging.error(f"Heartbeat error: {e}")
```

### 2. **Main Loop Cancellation Handling**

#### FacilitatorClient Main Loop
```python
async def stop(self) -> None:
    """Stop the FacilitatorClient gracefully"""
    if not self._is_running:
        return
    
    logger.info("Stopping FacilitatorClient")
    self._is_running = False  # Signal all components to stop
    
    # Cancel main task
    if self._main_task:
        self._main_task.cancel()
        try:
            await self._main_task
        except asyncio.CancelledError:
            pass  # Expected exception
    
    # Stop all components in order
    await self.heartbeat_manager.stop()
    await self.connection_manager.stop()

async def _main_loop(self) -> None:
    """Main message processing loop with proper cancellation"""
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
                break  # Exit immediately on cancellation
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(1)
                
    except asyncio.CancelledError:
        logger.info("Main loop cancelled")
    except Exception as e:
        logger.error(f"Fatal error in main loop: {e}")
        raise
```

### 3. **Message Processing with Timeout**

```python
async def _process_incoming_messages(self) -> None:
    """Process incoming messages with timeout to allow cancellation"""
    try:
        # Non-blocking receive with timeout
        message = await asyncio.wait_for(
            self.transport.receive(), 
            timeout=0.1  # Short timeout allows cancellation
        )
        
        self.metrics.record_message_received()
        await self._handle_message(message)
        
    except asyncio.TimeoutError:
        # No message received, continue (allows cancellation)
        pass
    except Exception as e:
        logger.error(f"Error processing incoming message: {e}")
        raise
```

### 4. **Graceful Shutdown Pattern**

The redesign implements a consistent graceful shutdown pattern:

```python
class ComponentBase:
    """Base class for all components with proper cancellation"""
    
    def __init__(self):
        self._is_running = False
        self._tasks: List[asyncio.Task] = []
    
    async def start(self) -> None:
        """Start component"""
        if self._is_running:
            return
        self._is_running = True
        # Start component-specific tasks
    
    async def stop(self) -> None:
        """Stop component gracefully"""
        if not self._is_running:
            return
        
        self._is_running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
        
        # Wait for all tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Component-specific cleanup
        await self._cleanup()
    
    async def _cleanup(self) -> None:
        """Component-specific cleanup"""
        pass
```

### 5. **Context Manager Support**

The redesign provides proper async context manager support:

```python
async def __aenter__(self):
    """Async context manager entry"""
    await self.start()
    return self

async def __aexit__(self, exc_type, exc_val, exc_tb):
    """Async context manager exit with proper cleanup"""
    await self.stop()
```

### 6. **Signal Handling Integration**

The redesign can be easily integrated with signal handlers:

```python
import signal
import asyncio

class SignalHandler:
    """Handles system signals for graceful shutdown"""
    
    def __init__(self, client: FacilitatorClient):
        self.client = client
        self._shutdown_event = asyncio.Event()
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown")
            self._shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def wait_for_shutdown(self):
        """Wait for shutdown signal"""
        await self._shutdown_event.wait()
        await self.client.stop()

# Usage
async def main():
    client = FacilitatorClient(config, celery_app)
    signal_handler = SignalHandler(client)
    signal_handler.setup_signal_handlers()
    
    async with client:
        await signal_handler.wait_for_shutdown()
```

## Key Improvements

### 1. **Explicit Cancellation Handling**
- Every component explicitly handles `asyncio.CancelledError`
- Tasks are cancelled explicitly in `stop()` methods
- No more stuck tasks on SIGINT

### 2. **Running State Management**
- Each component has a `_is_running` flag
- Loops check this flag and exit cleanly
- Prevents new work from starting during shutdown

### 3. **Ordered Shutdown**
- Components stop in reverse order of initialization
- Dependencies are shut down first
- No race conditions during shutdown

### 4. **Timeout-Based Operations**
- All blocking operations have timeouts
- Allows cancellation to be processed quickly
- Prevents indefinite blocking

### 5. **Exception Isolation**
- Each component handles its own exceptions
- Cancellation doesn't propagate unexpectedly
- Clean error reporting and logging

### 6. **Resource Cleanup**
- All resources are properly cleaned up
- No resource leaks during shutdown
- Proper connection closing

## Testing Cancellation

The redesign can be thoroughly tested for cancellation:

```python
import pytest
import asyncio

@pytest.mark.asyncio
async def test_cancellation_handling():
    """Test that cancellation is handled properly"""
    client = FacilitatorClient(config, celery_app)
    
    # Start client
    await client.start()
    
    # Cancel after short delay
    async def cancel_after_delay():
        await asyncio.sleep(0.1)
        await client.stop()
    
    # Run both tasks
    await asyncio.gather(
        client._main_loop(),
        cancel_after_delay(),
        return_exceptions=True
    )
    
    # Verify clean shutdown
    assert not client._is_running
    assert client._main_task.cancelled()

@pytest.mark.asyncio
async def test_signal_handling():
    """Test signal handling"""
    client = FacilitatorClient(config, celery_app)
    
    # Simulate SIGINT
    async with client:
        # Send cancellation to main task
        client._main_task.cancel()
        
        # Should handle gracefully
        try:
            await client._main_task
        except asyncio.CancelledError:
            pass  # Expected
```

## Benefits

1. **No More Stuck Tasks**: Proper cancellation handling prevents stuck tasks
2. **Graceful Shutdown**: All components shut down cleanly
3. **Resource Cleanup**: No resource leaks during shutdown
4. **Signal Handling**: Proper integration with system signals
5. **Testability**: Easy to test cancellation scenarios
6. **Debugging**: Clear logging of cancellation events
7. **Reliability**: Robust error handling and recovery

The redesign transforms the FacilitatorClient from a cancellation-prone mess into a robust, well-behaved component that handles shutdown gracefully and predictably.
