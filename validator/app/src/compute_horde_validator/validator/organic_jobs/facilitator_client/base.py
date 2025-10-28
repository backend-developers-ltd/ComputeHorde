import asyncio
import logging
import time

from .metrics import (
    VALIDATOR_FC_COMPONENT_STATE,
    VALIDATOR_FC_COMPONENT_UPTIME,
)
from .util import interruptible_wait, stop_task_gracefully


class BaseComponent:
    """
    Base class for all facilitator client components.
    """

    UPTIME_UPDATE_INTERVAL = 1.0

    def __init__(self) -> None:
        self._start_time = None
        self._stop_event = asyncio.Event()
        self._stop_event.set()  # Start stopped
        self._uptime_runner_task: asyncio.Task[None] | None = None
        self._logger = logging.getLogger(f"{__name__}.{self.name}")

    @property
    def name(self) -> str:
        return type(self).__name__

    async def _uptime_runner(self) -> None:
        """
        Update the uptime of the component in a dedicated thread to keep it
        separate from the functional logic of components.
        """
        self._start_time = time.monotonic()  # type: ignore[assignment]
        while self.is_running():
            if self._start_time is not None:
                uptime = time.monotonic() - self._start_time
                VALIDATOR_FC_COMPONENT_UPTIME.labels(component=self.name).set(uptime)
            await interruptible_wait(
                timeout=self.UPTIME_UPDATE_INTERVAL, stop_event=self._stop_event
            )

    def is_running(self) -> bool:
        """Checks if the component is running."""
        return not self._stop_event.is_set()

    async def start(self) -> None:
        """Starts the component."""
        if self.is_running():
            return

        self._stop_event.clear()
        self._uptime_runner_task = asyncio.create_task(self._uptime_runner())

        VALIDATOR_FC_COMPONENT_STATE.labels(component=self.name).set(1)

    async def stop(self) -> None:
        """Stops the component."""
        if not self.is_running():
            return

        self._stop_event.set()
        VALIDATOR_FC_COMPONENT_STATE.labels(component=self.name).set(0)

        try:
            await stop_task_gracefully(self._uptime_runner_task)
        except Exception as exc:
            self._logger.error("Error stopping uptime runner task: %s: %s", type(exc).__name__, exc)

        self._uptime_runner_task = None
