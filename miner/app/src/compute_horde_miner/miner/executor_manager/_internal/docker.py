import asyncio
import asyncio.subprocess
import contextlib
import dataclasses
import ipaddress
import logging
import socket
from collections import deque
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal, TypeAlias, cast

import aiodocker
import aiohttp
import asyncssh
import pydantic
import yaml
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings
from pydantic import Field, field_validator

from compute_horde_miner.miner.executor_manager._internal.base import BaseExecutorManager
from compute_horde_miner.miner.executor_manager.executor_port_dispenser import (
    executor_port_dispenser,
)

SSH_CONNECT_TIMEOUT = 15
SSH_KEEPALIVE_INTERVAL = 30
PULLING_TIMEOUT = 300  # TODO: align with corresponding validator timeout
DOCKER_STOP_TIMEOUT = 5
DOCKER_KILL_TIMEOUT = 5

logger = logging.getLogger(__name__)


class DockerExecutorConfigError(Exception): ...


class AllDockerExecutorsBusy(Exception): ...


class DockerExecutorFailedToStart(Exception): ...


class ServerConfigBase(pydantic.BaseModel):
    executor_class: ExecutorClass

    @field_validator("executor_class", mode="before")
    @classmethod
    def validate_default_executor_class(cls, value: Any) -> Any:
        if value == "DEFAULT_EXECUTOR_CLASS":
            return settings.DEFAULT_EXECUTOR_CLASS
        return value


class LocalServerConfig(ServerConfigBase):
    mode: Literal["local"] = "local"


class SSHServerConfig(ServerConfigBase):
    mode: Literal["ssh"] = "ssh"
    host: str
    ssh_port: int = 22
    username: str
    key_path: str


ServerConfig: TypeAlias = Annotated[
    LocalServerConfig | SSHServerConfig,
    Field(discriminator="mode"),
]
ServerName: TypeAlias = str
ServerConfigsPerClass: TypeAlias = dict[ExecutorClass, dict[ServerName, ServerConfig]]


class ServerManager:
    def __init__(self, path: str, cache_duration: timedelta = timedelta(minutes=5)) -> None:
        self._path: Path
        if path == "__default__":
            self._path = Path(__file__).parent.joinpath("default_docker_config.yaml")
        else:
            self._path = Path(path)

        if not self._path.is_file():
            raise DockerExecutorConfigError("configured path does not exist or is not a file")

        self._server_queue: dict[ExecutorClass, deque[ServerName]] = {
            executor_class: deque() for executor_class in ExecutorClass
        }
        self._reserved_servers: set[ServerName] = set()
        self._cache_duration = cache_duration
        self._cached_config: ServerConfigsPerClass = {}
        self._cached_config_at: datetime = datetime.min

    def fetch_config(self) -> ServerConfigsPerClass:
        if datetime.now() - self._cached_config_at < self._cache_duration:
            return self._cached_config

        # NOTE: We should probably read the file asynchronously.
        #       The cache helps, but it still can affect the asyncio loop.
        #       Differing until it actually starts affecting the event loop.
        with self._path.open() as f:
            raw_config = yaml.safe_load(f)

        try:
            config = pydantic.TypeAdapter(dict[ServerName, ServerConfig]).validate_python(
                raw_config
            )
        except pydantic.ValidationError as e:
            raise DockerExecutorConfigError("invalid config") from e

        if sum(1 for server_config in config.values() if server_config.mode == "local") > 1:
            raise DockerExecutorConfigError("multiple local servers are not supported")

        configs_per_class: ServerConfigsPerClass = {}
        for server_name, server_config in config.items():
            config_for_one_class = configs_per_class.setdefault(server_config.executor_class, {})
            config_for_one_class[server_name] = server_config

        self._cached_config = configs_per_class
        self._cached_config_at = datetime.now()

        return configs_per_class

    def reserve_server(self, executor_class: ExecutorClass) -> tuple[ServerName, ServerConfig]:
        fresh_config = self.fetch_config()
        queue = self._server_queue[executor_class]

        # add the new servers to the end of the queues
        for server_name in fresh_config.get(executor_class, {}):
            if not (server_name in queue or server_name in self._reserved_servers):
                queue.append(server_name)

        while queue:
            picked_server_name = queue.popleft()

            # check if the server is still in the config
            for server_name, server_config in fresh_config.get(executor_class, {}).items():
                if server_name == picked_server_name:
                    self._reserved_servers.add(server_name)
                    return server_name, server_config

        raise AllDockerExecutorsBusy("No free servers available!")

    def release_server(self, server_name: ServerName, server_config: ServerConfig) -> None:
        if server_name in self._reserved_servers:
            self._reserved_servers.remove(server_name)
            self._server_queue[server_config.executor_class].append(server_name)


@dataclasses.dataclass
class DockerExecutor:
    token: str
    container_id: str
    server_name: ServerName
    server_config: ServerConfig


class DockerExecutorManager(BaseExecutorManager):
    def __init__(self) -> None:
        if not settings.ADDRESS_FOR_EXECUTORS:
            raise DockerExecutorConfigError("ADDRESS_FOR_EXECUTORS is required!")

        super().__init__()
        self._server_manager = ServerManager(settings.DOCKER_EXECUTORS_CONFIG_PATH)

    async def start_new_executor(
        self, token: str, executor_class: ExecutorClass, timeout: float
    ) -> DockerExecutor:
        server_name, server_config = self._server_manager.reserve_server(executor_class)

        executor_image = settings.EXECUTOR_IMAGE
        if ":" not in executor_image:
            # aiodocker pulls all tags by default, so we need to specify the tag explicitly
            executor_image += ":latest"

        env = [
            f"MINER_ADDRESS=ws://{settings.ADDRESS_FOR_EXECUTORS}:{settings.PORT_FOR_EXECUTORS}",
            f"EXECUTOR_TOKEN={token}",
        ]
        if settings.HF_ACCESS_TOKEN is not None:
            env.append(f"HF_ACCESS_TOKEN={settings.HF_ACCESS_TOKEN}")
        if server_config.mode == "local":
            nginx_port = executor_port_dispenser.get_port()
            env.append(f"NGINX_PORT={nginx_port}")

        binds = [
            # the executor must be able to spawn images on host
            "/var/run/docker.sock:/var/run/docker.sock",
            "/tmp:/tmp",
        ]
        if settings.CUSTOM_JOB_RUNNERS_PATH:
            binds.append(
                f"{settings.CUSTOM_JOB_RUNNERS_PATH}:/root/src/compute_horde_miner/custom_job_runners.py"
            )

        cmdline_args = await self.get_executor_cmdline_args()

        try:
            async with tunneled_docker_client(server_config) as docker:
                # TODO: the executor image should already be on the server. Remove pulling from here?
                if not settings.DEBUG_SKIP_PULLING_EXECUTOR_IMAGE:
                    try:
                        await docker.images.pull(executor_image, timeout=PULLING_TIMEOUT)
                    except TimeoutError as e:
                        logger.error(
                            "Pulling executor container timed out, pulling it from shell might provide more details"
                        )
                        raise DockerExecutorFailedToStart("Failed to pull executor image") from e
                    except aiodocker.exceptions.DockerError as e:
                        logger.error("Failed to pull executor image: %r", e)
                        raise DockerExecutorFailedToStart("Failed to pull executor image") from e

                try:
                    container = await docker.containers.run(
                        config={
                            "Image": executor_image,
                            "Cmd": ["python", "manage.py", "run_executor", *cmdline_args],
                            "Env": env,
                            "HostConfig": {
                                "AutoRemove": settings.DEBUG_AUTO_REMOVE_EXECUTOR_CONTAINERS,
                                "Binds": binds,
                            },
                        },
                        name=token,
                    )
                except aiodocker.exceptions.DockerError as e:
                    logger.error("Docker container failed to start: %r", e)
                    raise DockerExecutorFailedToStart("Docker container failed to start") from e
                return DockerExecutor(token, container.id, server_name, server_config)
        except (Exception, asyncio.CancelledError):
            self._server_manager.release_server(server_name, server_config)
            raise

    async def get_executor_cmdline_args(self) -> list[str]:
        args = await super().get_executor_cmdline_args()
        if runner_cls := settings.CUSTOM_JOB_RUNNER_CLASS_NAME:
            args = [
                *args,
                "--job-runner-class",
                f"compute_horde_miner.custom_job_runners.{runner_cls}",
            ]
        return args

    async def kill_executor(self, executor: DockerExecutor) -> None:
        # kill executor container first so it would not be able to report anything - job simply timeouts
        logger.info("Stopping executor %s", executor.token)

        async with tunneled_docker_client(executor.server_config) as docker:
            job_container_name = f"{executor.token}-job"
            results = await asyncio.gather(
                _stop_or_kill_container(docker, executor.container_id),
                _stop_or_kill_container(docker, job_container_name),
                return_exceptions=True,
            )

        exceptions = [r for r in results if isinstance(r, BaseException)]
        if exceptions:
            # Do we want to propagate all the exceptions? BaseExceptionGroup?
            raise exceptions[0]

        self._server_manager.release_server(executor.server_name, executor.server_config)

    async def wait_for_executor(self, executor: DockerExecutor, timeout: float) -> int | None:
        async with tunneled_docker_client(executor.server_config) as docker:
            try:
                container = await docker.containers.get(executor.container_id)
                wait_result = await asyncio.wait_for(container.wait(), timeout=timeout)
                self._server_manager.release_server(executor.server_name, executor.server_config)
                exit_code: int | None = wait_result.get("StatusCode")
                if exit_code is None:
                    # The container somehow exited without an exit code! This should never happen.
                    logger.warning("Executor %s exited without an exit code!", executor.token)
                    exit_code = 3
                return exit_code
            except TimeoutError:
                return None
            except aiodocker.exceptions.DockerError as e:
                if e.status == 404:
                    self._server_manager.release_server(
                        executor.server_name, executor.server_config
                    )
                    # the container exited before, so we don't know the exit code
                    return 0
                else:
                    raise

    async def get_manifest(self) -> dict[ExecutorClass, int]:
        fresh_config = self._server_manager.fetch_config()
        return {
            executor_class: len(server_configs)
            for executor_class, server_configs in fresh_config.items()
        }

    async def get_executor_public_address(self, executor: DockerExecutor) -> str | None:
        if executor.server_config.mode == "ssh":
            return executor.server_config.host

        # local executors get their public address from the miner
        try:
            return await _get_miner_public_address()
        except Exception:
            logger.exception("Failed to get miner public address")
            return None


@contextlib.asynccontextmanager
async def tunneled_docker_client(server_config: ServerConfig) -> AsyncGenerator[aiodocker.Docker]:
    if server_config.mode == "local":
        # Skip tunneling if we're running locally
        async with aiodocker.Docker() as docker:
            yield docker
    else:
        async with asyncssh.connect(
            host=server_config.host,
            port=server_config.ssh_port,
            username=server_config.username,
            client_keys=[server_config.key_path],
            known_hosts=None,
            connect_timeout=SSH_CONNECT_TIMEOUT,
            keepalive_interval=SSH_KEEPALIVE_INTERVAL,
        ) as conn:
            listener = await conn.forward_local_port_to_path("", 0, "/var/run/docker.sock")
            local_port = listener.get_port()
            async with aiodocker.Docker(url=f"tcp://localhost:{local_port}") as docker:
                yield docker


_MISSING = object()
_cached_miner_address = _MISSING


async def _get_miner_public_address() -> str | None:
    """Get the miner's public address for a local executor."""
    global _cached_miner_address
    if _cached_miner_address is _MISSING:
        _cached_miner_address = await _internal_get_miner_public_address()
    return _cached_miner_address  # type: ignore[return-value]


async def _internal_get_miner_public_address() -> str | None:
    if not settings.BITTENSOR_MINER_ADDRESS_IS_AUTO:
        ip = ipaddress.ip_address(settings.BITTENSOR_MINER_ADDRESS)
        if ip.is_global:
            return cast(str, settings.BITTENSOR_MINER_ADDRESS)
        else:
            # most probably using ddos-shield, so we can't get the public address
            return None

    # If the miner address is set to auto, we can assume it's public.
    # Follows the implementation of the official bittensor SDK to get the public address,
    # but using only 2 sources instead of all the sources supported by the official SDK.
    candidate_sources = [
        "https://checkip.amazonaws.com",  # AWS
        "https://icanhazip.com/",  # Cloudflare
    ]
    connector = aiohttp.TCPConnector(family=socket.AF_INET)  # force IPv4 connection
    async with aiohttp.ClientSession(connector=connector) as session:
        for candidate_source in candidate_sources:
            last_exception = None
            try:
                async with session.get(candidate_source) as response:
                    raw_ip = await response.text()
                    raw_ip = raw_ip.strip()
                    ipaddress.IPv4Address(raw_ip)  # validate the IP address
                    return raw_ip
            except (aiohttp.ClientError, ValueError) as e:
                last_exception = e
        else:
            assert last_exception is not None  # mypy, Y U so naggy?
            raise last_exception


async def _stop_or_kill_container(docker: aiodocker.Docker, container_id: str) -> None:
    container = None
    try:
        container = await docker.containers.get(container_id, timeout=DOCKER_STOP_TIMEOUT)
        await asyncio.wait_for(container.stop(), timeout=DOCKER_STOP_TIMEOUT)
    except TimeoutError:
        if container is not None:
            await asyncio.wait_for(container.kill(), timeout=DOCKER_KILL_TIMEOUT)
    except aiodocker.exceptions.DockerError as e:
        if e.status != 404:
            raise
