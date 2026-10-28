Local Docker execution (v2)
===========================

``compute_horde_sdk.v2`` runs Linux containers on the configured Docker host.
It requires a running Docker daemon accessible to the caller and the SDK's base
dependencies. Before every run the executor checks the local image cache and
pulls the image if it is missing (or does not match the requested platform).
Existing matching images are reused without contacting the registry;
mutable tags are not refreshed. References without a tag use ``latest``.
Pulls use registry credentials from the caller's Docker configuration, including
configured credential helpers. There is no per-job credential option.
Pull failures are reported as ``LocalJobLaunchError``.

Docker connection
-----------------

``LocalDockerClient()`` uses ``docker.from_env()`` with its default settings.
Connection selection and API version negotiation are handled by the Docker SDK.


Submit, wait, and stop
----------------------

.. code-block:: python

    from pathlib import Path
    from compute_horde_sdk.v2 import (
        ComputeHordeJobSpec, LocalBindVolume, LocalDockerClient,
    )

    async def run_job():
        client = LocalDockerClient()
        job = await client.create_job(
            ComputeHordeJobSpec(
                docker_image="busybox:1.37",  # pulled automatically if missing
                args=["sh", "-c", 'cat /input/ref.fa; printf "done\\n" >&2'],
                volumes=[
                    LocalBindVolume(source=Path("/data/reference"), destination="/input"),
                    LocalBindVolume(source=Path("/data/results"), destination="/work", read_only=False),
                ],
                env={"EXAMPLE": "value with spaces"},
                cpus=2,
                memory="4g",
                user="1000:1000",             # omit to retain the image default
                platform="linux/amd64",       # omit to retain Docker's default
                execution_time_limit_sec=120,
            ),
            stdout=Path("/data/results/output.vcf"),
        )
        print(job.uuid, job.container_name)   # available immediately
        try:
            result = await job.wait()
            completed = result.to_completed_process()
            completed.check_returncode()     # optional nonzero-exit exception
            return completed
        finally:
            await job.stop()                  # repeatable, including after exit

``create_job`` returns before container creation finishes. ``container_id``
becomes available after creation; ``uuid`` and ``container_name`` are available
earlier. A custom ``container_name`` is supported. Docker containers carry the
``io.computehorde.sdk.job`` label with the job UUID. A name collision fails
launch and never removes the other container.

Arguments are a sequence of individual strings. The SDK preserves the image's
ENTRYPOINT and passes arguments as Docker's Cmd array. An empty sequence leaves
the image's CMD intact. Shell syntax is interpreted only when the caller
explicitly runs a shell, as in the example. No shell is involved in stopping.

Bind volumes and execution policy
---------------------------------

``LocalBindVolume`` is an immutable, validated volume object with ``source``,
``destination``, and ``read_only=True``. Sources must already be regular files
or directories. Relative sources become absolute at construction. Destinations
are absolute Linux container paths, without v1's ``/volume/`` or ``/output/``
prefix restrictions. The volume list preserves repeated sources, distinct
destinations, and mixed modes. Docker rejects conflicting destinations.

The original host paths are bound directly. Nothing is staged, copied, scanned,
cached, registered by URL, or uploaded. A writable mount may serve as both input
and output; generated files are immediately visible on the host. Filesystem
ownership and permissions follow ordinary Docker bind behavior. Validation does
not lock host paths against subsequent replacement; keep sources stable during
submission. Bind paths refer to the Docker daemon's host; when using a remote
daemon, the same paths must also exist there. Source validation checks the
caller's filesystem.

``cpus`` sets a per-container CPU quota (Docker NanoCpus, equivalent to
``--cpus``). ``memory`` sets a per-container byte limit, accepting positive bytes
or Docker sizes such as ``512m`` and ``4g``. These are not machine allocation or
tool thread counts. Unset CPU, memory, user, and platform options retain Docker
or image defaults.

Containers use Docker's default networking. There is no per-job network
configuration option.

The SDK attaches to stdout/stderr before starting the container and removes the
container after collecting its output and exit status. Docker logging is disabled
for these containers; output goes to the SDK or the supplied stdout destination.
Containers are also force-removed on cancellation, timeout, and launch failure,
including created containers that never started. Anonymous Docker volumes are
removed; bind contents are retained.

There is no stdin forwarding: stdin is closed. There are no privileged, device,
GPU, port, IPC, working-directory, or ENTRYPOINT overrides. Windows runtime,
remote Compute Horde execution, and cloud fallback are outside this API.

Output and results
------------------

Without ``stdout=``, stdout and stderr are captured in full in memory. With
``stdout=`` set to a path or open file object, stdout is written incrementally
with backpressure and is **not** accumulated in the result; ``result.stdout``
is ``None``. Stderr is always captured separately in full. Large stdout artifacts
should use a file; large stderr still consumes memory.

The default is bytes. ``text=True`` decodes captured streams with ``encoding``
(default UTF-8) and ``errors`` (default strict). Text mode does not normalize
newlines. An open stdout file must accept bytes when ``text=False`` or strings
when ``text=True``; incremental decoding preserves multibyte characters across
chunks. A path destination always receives exact original bytes. The SDK opens
paths in overwrite mode and closes its own files. Caller-provided objects are
flushed but not closed; keep them open until the job finishes. Writes run off
the event loop, allowing another task to stop a job during file I/O.

``wait()`` returns ``LocalJobResult`` and also sets ``job.result``. The result
contains ``args`` (image followed by arguments), ``returncode``, ``stdout``,
``stderr``, and ``status``. ``to_completed_process()`` adapts it to
``subprocess.CompletedProcess`` when an actual exit status is available.

* Exit 0: ``COMPLETED`` result.
* Nonzero exit, including 125 or 137: ``FAILED`` result with the actual code;
  no exception unless the caller invokes ``check_returncode()``.
* Image preparation, container creation, attachment, or startup failure:
  ``LocalJobLaunchError``.
* Output transport, exit-status retrieval, decoding, or file-write failure:
  ``LocalJobExecutionError``.
* Explicit stop: ``LocalJobCancelledError`` from ``wait()``.
* Execution deadline or ``wait(timeout=...)``: ``LocalJobTimeoutError`` after
  cleanup is confirmed.
* Unconfirmed removal: ``LocalCleanupError`` or ``LocalCleanupTimeoutError``.
  Cleanup failures take precedence over the execution error; inspect the
  exception's ``result`` when present, retain the handle, and retry ``stop()``.

Exceptions expose any available partial result. Cancellation or launch failure
may have ``returncode=None``; the SDK does not invent an exit code. Decode or
sink errors may prevent constructing a result. ``stop()`` succeeds after
container creation and startup have settled, any owned container has been
removed, and output handling has finished. Image preparation may still be
finishing in the background; it cannot create a container after cancellation.

The execution timer starts when the Docker start request completes; image
pulling, container creation, and attachment are not included.
``wait(timeout=...)`` bounds the entire wait, requests cancellation, and then
allows up to ``cleanup_timeout`` (default 30 seconds) for cleanup. A hung
daemon can prevent confirming removal: the SDK reports a cleanup failure instead
of claiming the container stopped.
Stopping during image preparation prevents container creation. An in-flight
Docker call may continue until it returns or produces another pull progress
message, at which point preparation releases its resources. Docker may retain
downloaded image layers for future jobs.
If a create response is lost, an absent container name does not prove that
creation will not finish later. Cleanup remains unconfirmed until the owned
container can be found and removed; retain the handle for retry or reconcile
the UUID label with the daemon after connectivity is restored.

Version boundary and installation
---------------------------------

v1 exports, HTTP/download volumes, and native remote behavior are unchanged.
The v2 job spec is separate, because local binds do not implement v1's transfer
serialization contract. Native and fallback clients, including fallback spec
conversion, explicitly reject v2 specs before making requests or staging data.

Validation
----------

From ``compute_horde_sdk/``, run ``uv run nox -s test-3.11`` and
``uv run nox -s type_check``. Unit tests mock the daemon and cover argument and
option mapping, bindings, exact streams, lifecycle races, and cleanup deadlines.
Disposable real Docker tests are opt-in:

.. code-block:: console

    CH_SDK_DOCKER_TESTS=1 uv run --group test pytest tests/integration/test_local_docker.py

These build and remove a small ENTRYPOINT test image and run disposable
containers. The automatic-pull smoke test runs when ``busybox:1.37`` is absent
and skips if it is already cached.
