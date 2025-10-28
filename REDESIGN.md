# FacilitatorClient redesign

## Problem statement

The facilitator connector is in a sorry state:
- No transport abstraction - Refactor FacilitatorClient to use an abstract Transport instead of straight Webstocket
- Randomly disconnects, but the errors do not indicate the issue
- Convoluted connect / send message retry logic, effectively nested retries
- Doesn’t handle asyncio cancellation well, gets stuck on SIGINTs
- Doesn’t emit any metrics or system events
- Is a mess of concurrent asyncio tasks
- Undocumented and surprising (e.g. requires an “async with” context before entering the loop, but doesn’t document or check for it)
- Makes no effort to preserve the order of queued messages after reconnecting to faci (important for status updates)

## General function

The general function of the `FacilitatorClient` remains as follows:
- Send periodic heartbeats to the facilitator
- Listen for updates on executor machine specs and forward them to the facilitator
- Listen for websocket messages from the facilitator in an endless loop. These messages can be one of:
    - job requests
    - cheated job  reports
    - acknowledgements from the facilitator that messages from the validator were received
- Listen for updates of submitted jobs and forward them to the facilitator

## Redesign overview

To address these issues and improve maintainability, this design aims to make the FacilitatorClient more modular. The following core components are part of the new redesign:

- **TransportLayer**: an abtraction of the websocket connection (or any other connection method). The existing `compute_horde.transport.ws.WSTransport` can be used for this with minor upgrades (e.g. adding a `is_connected()` method for easier management)
- **ConnectionManager**: handles connection tracking and reconnection logic.
- **MessageManager**: manages and sends messages meant for the facilitator over a transport channel. It ensures that message order remains consistent despite disconnects/reconnects to the facilitator and will only try to send a message if the connection is active. All other components will pass messages to be sent to the queue.
- **HeartbeatManager**: sends periodic heartbeats along a transport layer.
- **JobStatusUpdateManager**: listens for update messages from submitted jobs and adds them as messages to the message queue to be sent to the facilitator.
- **SpecUpdateManager**: listens for machine spec message to set/update the executor specs and adds them as messages to the message queue to be sent to the facilitator.
- **JobDispatcher**: a simple utility class that dispatches jobs into a message queue, where they can be picked up by the proposed Routing/Jobs modules of the redesign.
- **PerformanceMonitor**: collects metrics related to operation, e.g. number of messages sent, disconnects and reconnects to the facilitator, etc., and emits SystemEvents.

## Migration strategy

To ensure compatibility with existing code, the migration can be split into two distinct phases

### Phase 1: Internal-only components
This phase impacts only "internal" components as well as usage of the `FacilitatorClient` itself and requires only minimal changes to the code base.

- Implement:
    - `ConnectionManager`
    - `MessageManager` 
    - `JobStatusUpdateManager`
    - `HeartbeatManager`
    - `SpecUpdateManager`
    - `PerformanceMonitor`
- Refactor the `FacilitatorClient` to use these as well as the existing `compute_horde.transport.ws.WSTransport` class.
- Implement an interim version of the `JobDispatcher` that still uses the current logic, i.e. the `FacilitatorClient` determines the job route itself and submits the job to the miner directly (see `FacilitatorClient._process_job_request`)

This redesign does not impact how the `FacilitatorClient` interacts with outside components, e.g. the facilitator or miners, and will not require any other components to be adjusted (sans minor usability additions, e.g. a connection status method for the transport layer)

### Phase 2: Upgrade JobDispatcher
Once the new Routing/Jobs components exist, the JobDispatcher class can be updated to have the intended functionality, i.e. submit tasks to a Celery message queue and not be concerned with the actual logic of routing and submitting jobs

## How current issues are addressed

- Uses transport abstraction that decouples it from the websocket logic itself.
- Connection and message sending logic is cleanly separated into separate classes. The MessageManager can be instructed to wait until an active connection exists, ensuring that send-message retries are only triggered if the message actually fails to send, and not if the connection is closed, as is currently the case.
- asyncio cancellation will be explicitly handled by all components by ensuring that they cease operation if an `asyncio.CancelledError` is raised.
- Metrics and system events will be captured by the PerformanceMonitor, hopefully enabling more details on why connections fail and avoiding the current issue of disconnects occurring without known issue.
- Each component manages its own lifecycle, cleaning up the current asyncio task spaghetti. Furthermore, the job dispatcher will avoid the dynamic generation of asyncio tasks as it simply pushes messages into a celery queue for other components to handle (in the final version, at least. In the interim, it will still need to create individual tasks for each job submission but at least the nested spawning of job status update listeners can be avoided)
- The `FacilitatorClient` entrypoint is cleaned up to eliminate the need for the current convoluted usage (`async with` and then `await facilitator_client.run_forever()` inside the context). Instead, `await client.start()` will set up the client and start the main operating loop while the teardown will be handled automatically via `try-except-finally` logic in the main loop.
- Message order is preserved via the `MessageManager` class.

## Class skeletons

This section outlines the individual components' functionality. Nothing here is final, yet, and may very well change as issues are identified during implementation. The overall functionality should remain consistent with the design, though.

### TransportLayer

See `compute_horde.transport.ws.WSTransport`

Only minor additions are necessary, e.g. a method to check the connection status.

### ConnectionManager

```Python
class ConnectionManager:
    """
    Periodically checks that the connection across a transport layer is still
    active and reconnects
    """

    def start():
        """Sets up the connection management and starts the main loop"""
    def _main_loop():
        """
        Main "run forever" loop that checks that the connection is still active
        and triggers reconnection attempts if not. Emits SystemEvents whenever
        a disconnect is detected
        """
    def stop():
        """Ends connection management and disconnects the transport layer"""
```

### MessageManager

```Python
class MessageManager:
    """
    Periodically checks whether any messages are queued and attempts to send 
    these across a transport layer.
    """
    def enqueue_message():
        """Adds message to the end of the queue"""
    def get_next_message():
        """Gets the oldest message in the queue (FIFO principle)"""
    def retry_message():
        """
        Inserts message into the front of the queue again to retry sending it.
        Keeps track of how often a message was retried for logging and 
        optionally emits a SystemEvent (or exception?) if a message cannot be
        sent
        """
    def start():
        """Sets up the message queue and starts the main loop"""
    def _main_loop():
        """
        Main "run forever" loop that attempts to send all queued messages in 
        their order. Will retry resending messages if they fail to send and 
        can optionally raise errors or send system events indicating a message
        could not be sent. Will only attempt to send messages if the connection
        is active
        """
    def stop():
        """
        Ends message manager. Attempts to send all remaining messages to clear 
        the queue
        """
```

### JobStatusUpdateManager

```Python
class JobStatusUpdateManager:
    """
    Listens for job updates and generates messages for the message manager from
    these. The main listener loop is started as a separate task for each job.
    """
    def start_listener_task(job_uuid):
        """Start a listener task for a given job uuid"""
    def _listener_loop(job_uuid):
        """
        Main "run forever" loop that listens for updates for a specific job 
        uuid. Generates messages from these updates and passes them onto the
        message manager
        """
    def stop_listener_task(job_uuid):
        """Stop a listening task for a given job uuid and clean up the task"""
```

### HeartbeatManager

```Python
class HeartbeatManager:
    def start():
        """Sets up the heartbeat management and starts the main loop"""
    def _main_loop():
        """
        Main "run forever" loop that periodically adds heartbeat message to
        the MessageManager
        """
    def stop():
        """Ends heartbeat management and disconnects the transport layer"""
```

### SpecUpdateManager

```Python
class SpecUpdateManager:
    def start():
        """Sets up the machine spec update manager and starts the main loop"""
    def _main_loop():
        """
        Main "run forever" loop that listens for machine spec messages and 
        passes them onto the message manager.
        """
    def stop():
        """Ends machine spec update management"""
```

### PerformanceMonitor

```Python
class PerformanceMonitor:
    """
    This is really just a glorified data class that keeps track of metrics,
    like the number of messages sent. Exact details will depend on how metrics
    should be logged or propagated.
    """
```

### JobDispatcher

```Python
class JobDispatcher:
    def dispatch_job():
        """
        Submit a job.
        
        For the interim version of the job dispatcher, this method can be 
        hijacked to utilize the old logic, i.e. the
        FacilitatorClient.process_job_request functionality. This will make
        migration to the message-dispatching system painless.

        In the final version, a message would be sent to the Routing/Jobs 
        components and these would handle the actual execution of the job.
        """
```

### FacilitatorClient

```Python
class FacilitatorClient:
    def __init__():
        """
        Set up all components, e.g. connection manager, message manager, etc.
        """
    
    def start():
        """
        Start all run-forever components, authenticate with the facilitator,
        and start the main process loop
        """
    
    def _main_loop():
        # Listen for messages from facilitator
        
        # If message is Response, check that the status is "success"
        # If message is OrganicJobRequest:
        #   forward it to the job dispatcher
        #   create update listener for the job
        # If the message is V0JobCheated, process it according to the current logic
    
    def stop():
        """End the main process loop and stop all run-forever components"""
        