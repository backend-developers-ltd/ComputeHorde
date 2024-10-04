import abc
import logging
from typing import Any, Self, TypeVar

import pydantic
from channels.generic.websocket import AsyncWebsocketConsumer
from compute_horde.base.output_upload import OutputUpload
from compute_horde.base.volume import Volume
from compute_horde.mv_protocol import validator_requests
from compute_horde.utils import MachineSpecs
from pydantic import model_validator

from compute_horde_miner.miner.miner_consumer.base_compute_horde_consumer import (
    log_errors_explicitly,
)

logger = logging.getLogger(__name__)


class ExecutorReady(pydantic.BaseModel):
    executor_token: str


class ExecutorFailedToPrepare(pydantic.BaseModel):
    executor_token: str


class JobRequest(pydantic.BaseModel):
    job_uuid: str
    docker_image_name: str | None = None
    raw_script: str | None = None
    docker_run_options_preset: str
    docker_run_cmd: list[str]
    volume: Volume | None = None
    output_upload: OutputUpload | None = None

    @model_validator(mode="after")
    def validate_at_least_docker_image_or_raw_script(self) -> Self:
        if not (bool(self.docker_image_name) or bool(self.raw_script)):
            raise ValueError("Expected at least one of `docker_image_name` or `raw_script`")
        return self


class ExecutorSpecs(pydantic.BaseModel):
    job_uuid: str
    specs: MachineSpecs


class ExecutorFinished(pydantic.BaseModel):
    job_uuid: str
    docker_process_stdout: str
    docker_process_stderr: str


class ExecutorFailed(pydantic.BaseModel):
    job_uuid: str
    docker_process_exit_status: int | None = None
    docker_process_stdout: str
    docker_process_stderr: str


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
        payload = self.validate_event("executor_ready", ExecutorReady, event)
        if payload:
            await self._executor_ready(payload)

    @abc.abstractmethod
    async def _executor_ready(self, msg: ExecutorReady): ...

    @log_errors_explicitly
    async def executor_failed_to_prepare(self, event: dict[str, Any]):
        payload = self.validate_event("executor_failed_to_prepare", ExecutorFailedToPrepare, event)
        if payload:
            await self._executor_failed_to_prepare(payload)

    @abc.abstractmethod
    async def _executor_failed_to_prepare(self, msg: ExecutorFailedToPrepare): ...

    @log_errors_explicitly
    async def executor_finished(self, event: dict[str, Any]):
        payload = self.validate_event("executor_finished", ExecutorFinished, event)
        if payload:
            await self._executor_finished(payload)

    @abc.abstractmethod
    async def _executor_specs(self, event: ExecutorSpecs): ...

    @log_errors_explicitly
    async def executor_specs(self, event: dict[str, Any]):
        payload = self.validate_event("executor_specs", ExecutorSpecs, event)
        if payload:
            await self._executor_specs(payload)

    @abc.abstractmethod
    async def _executor_finished(self, msg: ExecutorFinished): ...

    @log_errors_explicitly
    async def executor_failed(self, event: dict[str, Any]):
        payload = self.validate_event("executor_failed", ExecutorFailed, event)
        if payload:
            await self._executor_failed(payload)

    @abc.abstractmethod
    async def _executor_failed(self, msg: ExecutorFailed): ...

    async def send_job_request(self, executor_token, job_request: validator_requests.V0JobRequest):
        await self.channel_layer.group_send(
            ExecutorInterfaceMixin.group_name(executor_token),
            {
                "type": "miner.job_request",
                **JobRequest(
                    job_uuid=job_request.job_uuid,
                    docker_image_name=job_request.docker_image_name,
                    raw_script=job_request.raw_script,
                    docker_run_options_preset=job_request.docker_run_options_preset,
                    docker_run_cmd=job_request.docker_run_cmd,
                    volume=job_request.volume,
                    output_upload=job_request.output_upload,
                ).model_dump(),
            },
        )


class ExecutorInterfaceMixin(BaseMixin):
    @classmethod
    def group_name(cls, executor_token: str):
        return f"executor_interface_{executor_token}"

    async def send_executor_ready(self, executor_token: str):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {
                "type": "executor.ready",
                **ExecutorReady(executor_token=executor_token).model_dump(),
            },
        )

    async def send_executor_failed_to_prepare(self, executor_token: str):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {
                "type": "executor.failed_to_prepare",
                **ExecutorFailedToPrepare(executor_token=executor_token).model_dump(),
            },
        )

    async def send_executor_specs(self, job_uuid: str, executor_token: str, specs: MachineSpecs):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {
                "type": "executor.specs",
                **ExecutorSpecs(
                    job_uuid=job_uuid,
                    specs=specs,
                ).model_dump(),
            },
        )

    async def send_executor_finished(
        self, job_uuid: str, executor_token: str, stdout: str, stderr: str
    ):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {
                "type": "executor.finished",
                **ExecutorFinished(
                    job_uuid=job_uuid,
                    docker_process_stdout=stdout,
                    docker_process_stderr=stderr,
                ).model_dump(),
            },
        )

    async def send_executor_failed(
        self, job_uuid: str, executor_token: str, stdout: str, stderr: str, exit_status: int | None
    ):
        group_name = ValidatorInterfaceMixin.group_name(executor_token)
        await self.channel_layer.group_send(
            group_name,
            {
                "type": "executor.failed",
                **ExecutorFailed(
                    job_uuid=job_uuid,
                    docker_process_stdout=stdout,
                    docker_process_stderr=stderr,
                    docker_process_exit_status=exit_status,
                ).model_dump(),
            },
        )

    @abc.abstractmethod
    async def _miner_job_request(self, msg: JobRequest): ...

    @log_errors_explicitly
    async def miner_job_request(self, event: dict[str, Any]):
        payload = self.validate_event("miner_job_request", JobRequest, event)
        if payload:
            await self._miner_job_request(payload)
