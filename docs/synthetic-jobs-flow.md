### Synthetic jobs test flow

We will move all **DB** accesses before/after the actual for loop which sends jobs. We will not do any **DB** access during the main part where we talk to the miners.

**All timeout values are approximate, exact values to be determined.**

### Get miners

* get list of miners (hotkey, ip, port)
* possibly from cache/db, updated regularly by celery task. This would allow us to send jobs even if subtensor is not accessible for a while. This creates some issues with new/deregistered miners.

### Pregenerate jobs

* **DB**: get total number of synthetic jobs used by the previous batch

* pregenerate (in memory) 20% more synthetic jobs than in the previous batch (extra headroom)

* **DB**: create new SyntheticJobBatch model

### Get manifests

```
asyncio.gather: (all tasks created below)
    for miner in randomized(miners):
        asyncio.create_task: (task async code below)
            with timeout(30 sec):
                connect to miner WS
                get manifest
                disconnect
```

* compute final number of required synthetic jobs
* generate more jobs if we don't have enough pregenerated ones

### Check for availability

We want to test all executors in a synchronized way, at the end of the spin-up time. So we stagger the requests, sleep less for executor classes will long spin-up time.

|class/minute |0|1|2|3|4|
|-            |-|-|-|-|-|
|4 min spin-up|X| | | | |
|always on    | | | | |X|

X marks the time when we send the **initial** job request. `cost-effective` class (4 min spin-up) will have more time to prepare, since we contact it before the `always-on` class:

```
asyncio.gather: (all tasks created below)
    for executor in randomized(executors):
        asyncio.create_task: (task async code below)
            with timeout(max spin-up time of all classes):
                sync barrier

                asyncio.sleep for staggered start according to discussion above

                connect miner WS

                send V0InitialJobRequest
                await for accept/decline

                if accept:
                    send job started receipt

                keep miner client (WS connection) open for next stage

discard executors which declined/timed out.
```

If all the tasks from above finish before the timeout (spin up time), sleep the rest of the period. Example: if the longest spin-up time across executor classes returned in the manifests is 4 min, but all executors accept/decline the initial job request in 3 minutes, sleep for another 1 minute.

We send the job started receipts before the `V0JobRequest` message, since if the miner accepts the executor is considered reserved, including during the 4 min spin-up time.

### Send actual jobs

At this point, all executors accepted or declined the initial job offers. Now send the jobs, at the same time for all executors of all classes:

```
asyncio.gather: (all tasks created below)
    for executor in randomized(executors):
        asyncio.create_task: (task async code below)
            timeout(1 min)
                serialize V0JobRequest to json

                sync barrier

                job_before_send_request_time = now
                send already serialized V0JobRequest
                job_after_send_request_time = now
                await for job finished or failed
                job_end_time = now
```

We are creating thousands of tasks, which has some overhead. To ensure they all start sending the requests at the same time, we use a sync barrier inside the task.

We record both the time just before sending the request and immediately after sending it. This allows us to detect when sending the message takes a really long time due to asyncio/websocket library issues. If the time is very long, we will emit a system event.

### Send job finished receipts

Send jobs finished receipts to all successful executors, after all the tasks above finish, so we don't put pressure on the event loop during the critical period where we measure time.

### Update DB

* **DB**: bulk create SyntheticJob models (from used pregenerated ones)
* **DB**: bulk create JobStartedReceipt/JobFinishedReceipts sent to miners above
* **DB**: bulk create system events emitted at any point above. To ensure we don't lose system events if the synthetic jobs function crashes we'll collect them in some list/queue

### Score jobs

Do the scoring according to the job results captured above.

### Misc

To avoid potentially giving a small advantage to miners which are returned first from the metagraph, we randomize the order we create miner/executor tasks on each run.

We can increase timing accuracy (fairness) by using multiple Python processes, each one serving only 50-100 executors instead of a single process serving thousands of them. This would require a form of inter-process synchronization (System V, pthreads, redis, ...).
