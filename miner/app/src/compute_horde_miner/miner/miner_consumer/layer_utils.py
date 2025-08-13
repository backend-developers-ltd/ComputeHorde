import abc
import logging
from typing import Any, TypeVar

import pydantic
from channels.generic.websocket import AsyncWebsocketConsumer
from compute_horde.protocol_messages import (
    V0ExecutionDoneRequest,
    V0ExecutorReadyRequest,
    V0HordeFailedRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
    V0JobRequest,
    V0MachineSpecsRequest,
    V0StreamingJobReadyRequest,
    V0VolumesReadyRequest,
)

from compute_horde_miner.miner.miner_consumer.base_compute_horde_consumer import (
    log_errors_explicitly,
)

logger = logging.getLogger(__name__)
TModel = TypeVar("TModel", bound=pydantic.BaseModel)


class BaseMixin(AsyncWebsocketConsumer, abc.ABC):
    @classmethod
    @abc.abstractmethod
    def group_name(cls, executor_token: str) -> str: ...

    async def group_add(self, executor_token: str):
        group_name = self.group_name(executor_token)
        await self.channel_layer.group_add(group_name, self.channel_name)

    async def group_discard(self, executor_token: str):
        group_name = self.group_name(executor_token)
        await self.channel_layer.group_discard(group_name, self.channel_name)

    def validate_event(
        self, event_type, models_class: type[TModel], event: dict[str, Any]
    ) -> TModel | None:
        try:
            return models_class(**event)
        except Exception:
            logger.error(f"Encountered when processing {event_type} layer message:", exc_info=True)
            return None


class ValidatorInterfaceMixin(BaseMixin, abc.ABC):
    @classmethod
    def group_name(cls, executor_token: str):
        return f"validator_interface_{executor_token}"

    @log_errors_explicitly
    async def executor_ready(self, event: dict[str, Any]):
        payload = self.validate_event("executor_ready", V0ExecutorReadyRequest, event)
        if payload:
            await self._executor_ready(payload)

    @abc.abstractmethod
    async def _executor_ready(self, msg: V0ExecutorReadyRequest): ...

    @log_errors_explicitly
    async def streaming_job_ready(self, event: dict[str, Any]):
        payload = self.validate_event("streaming_job_ready", V0StreamingJobReadyRequest, event)
        if payload:
            await self._streaming_job_ready(payload)

    @abc.abstractmethod
    async def _streaming_job_ready(self, msg: V0StreamingJobReadyRequest): ...

    @log_errors_explicitly
    async def executor_finished(self, event: dict[str, Any]):
        payload = self.validate_event("executor_finished", V0JobFinishedRequest, event)
        if payload:
            await self._executor_finished(payload)

    @abc.abstractmethod
    async def _executor_specs(self, event: V0MachineSpecsRequest): ...

    @log_errors_explicitly
    async def executor_specs(self, event: dict[str, Any]):
        payload = self.validate_event("executor_specs", V0MachineSpecsRequest, event)
        if payload:
            await self._executor_specs(payload)

    @abc.abstractmethod
    async def _executor_finished(self, msg: V0JobFinishedRequest): ...

    @log_errors_explicitly
    async def job_failed(self, event: dict[str, Any]):
        payload = self.validate_event("job_failed", V0JobFailedRequest, event)
        if payload:
            await self._job_failed(payload)

    @abc.abstractmethod
    async def _job_failed(self, msg: V0JobFailedRequest): ...

    @log_errors_explicitly
    async def horde_failed(self, event: dict[str, Any]):
        payload = self.validate_event("horde_failed", V0HordeFailedRequest, event)
        if payload:
            await self._horde_failed(payload)

    @abc.abstractmethod
    async def _horde_failed(self, msg: V0HordeFailedRequest): ...

    async def send_job_request(self, executor_token, job_request: V0JobRequest):
        await self.channel_layer.group_send(
            ExecutorInterfaceMixin.group_name(executor_token),
            {"type": "miner.job_request", **job_request.model_dump()},
        )

    @log_errors_explicitly
    async def volumes_ready(self, event: dict[str, Any]) -> None:
        payload = self.validate_event("volumes_ready", V0VolumesReadyRequest, event)
        if payload:
            await self._volumes_ready(payload)

    @abc.abstractmethod
    async def _volumes_ready(self, msg: V0VolumesReadyRequest): ...

    @log_errors_explicitly
    async def execution_done(self, event: dict[str, Any]) -> None:
        payload = self.validate_event("execution_done", V0ExecutionDoneRequest, event)
        if payload:
            await self._execution_done(payload)

    @abc.abstractmethod
    async def _execution_done(self, msg: V0ExecutionDoneRequest): ...


class ExecutorInterfaceMixin(BaseMixin):
    @classmethod
    def group_name(cls, executor_token: str):
        return f"executor_interface_{executor_token}"

    async def send_executor_ready(self, executor_token: str, msg: V0ExecutorReadyRequest):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        msg = msg.model_copy(update={"executor_token": executor_token})
        await self.channel_layer.group_send(
            group_name,
            {"type": "executor.ready", **msg.model_dump()},
        )

    async def send_streaming_job_ready(self, executor_token: str, msg: V0StreamingJobReadyRequest):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        msg = msg.model_copy(update={"executor_token": executor_token})
        await self.channel_layer.group_send(
            group_name,
            {"type": "streaming_job.ready", **msg.model_dump()},
        )

    async def send_volumes_ready(self, executor_token: str, msg: V0VolumesReadyRequest):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {"type": "volumes.ready", **msg.model_dump()},
        )

    async def send_execution_done(self, executor_token: str, msg: V0ExecutionDoneRequest):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {"type": "execution.done", **msg.model_dump()},
        )

    async def send_executor_specs(self, executor_token: str, msg: V0MachineSpecsRequest):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {"type": "executor.specs", **msg.model_dump()},
        )

    async def send_executor_finished(self, executor_token: str, msg: V0JobFinishedRequest):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {"type": "executor.finished", **msg.model_dump()},
        )

    async def send_horde_failed(self, executor_token: str, msg: V0HordeFailedRequest):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {"type": "horde.failed", **msg.model_dump()},
        )

    async def send_job_failed(self, executor_token: str, msg: V0JobFailedRequest):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {"type": "job.failed", **msg.model_dump()},
        )

    @abc.abstractmethod
    async def _miner_job_request(self, msg: V0JobRequest): ...

    @log_errors_explicitly
    async def miner_job_request(self, event: dict[str, Any]):
        payload = self.validate_event("miner_job_request", V0JobRequest, event)
        if payload:
            await self._miner_job_request(payload)
