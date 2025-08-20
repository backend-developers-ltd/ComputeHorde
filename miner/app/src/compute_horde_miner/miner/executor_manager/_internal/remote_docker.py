import asyncio
import logging
from collections import deque
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, TypeAlias, TypedDict

import asyncssh
import yaml
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings

from compute_horde_miner.miner.executor_manager._internal.base import (
    BaseExecutorManager,
    ExecutorUnavailable,
)

PULLING_TIMEOUT = 300
DOCKER_STOP_TIMEOUT = 5

logger = logging.getLogger(__name__)


class _ServerConfig(TypedDict):
    host: str
    key: str
    username: str
    executor_class: str


_ServerNameToConfig: TypeAlias = dict[str, _ServerConfig]
_Configs: TypeAlias = dict[str, _ServerNameToConfig]


class RemoteDockerExecutor:
    def __init__(
        self,
        process_executor: Any,
        token: str,
        conn: Any,
        server_name: str,
        server_config: _ServerConfig,
    ) -> None:
        self.process_executor = process_executor
        self.token = token
        self.conn = conn
        self.server_name = server_name
        self.server_config = server_config


class RemoteServers:
    def __init__(self) -> None:
        self._configs: _Configs = {}
        self._server_queues: dict[str, deque[str]] = {
            str(executor_class): deque() for executor_class in ExecutorClass
        }
        self._reserved_servers: set[str] = set()

    @property
    def configs(self) -> _Configs:
        config_path = settings.REMOTE_DOCKER_EXECUTORS_CONFIG_PATH
        if not config_path:
            raise RuntimeError("REMOTE_DOCKER_EXECUTORS_CONFIG_PATH is not configured")

        with open(config_path) as fp:
            new_config = yaml.safe_load(fp)

        # Update the server queue based on the new config
        for executor_class_str in self._server_queues:
            executor_class_configs = {}
            for server_name, server_config in new_config.items():
                if server_config["executor_class"] == executor_class_str:
                    executor_class_configs[server_name] = server_config
            self._update_server_queue(executor_class_str, executor_class_configs)
            self._configs[executor_class_str] = executor_class_configs

        return self._configs

    def _update_server_queue(
        self, executor_class_str: str, new_config: _ServerNameToConfig
    ) -> None:
        # Remove servers that are no longer in the config
        self._server_queues[executor_class_str] = deque(
            [server for server in self._server_queues[executor_class_str] if server in new_config]
        )
        self._reserved_servers = {
            server for server in self._reserved_servers if server in new_config
        }

        # Add new servers that are not in the queue and not reserved
        new_servers = (
            set(new_config.keys())
            - set(self._server_queues[executor_class_str])
            - self._reserved_servers
        )
        self._server_queues[executor_class_str].extend(new_servers)

    def reserve_server(self, executor_class_str: str) -> tuple[str, _ServerConfig]:
        while self._server_queues[executor_class_str]:
            server_name = self._server_queues[executor_class_str].popleft()
            # Double-check if the server is still in config
            if server_name in self._configs[executor_class_str]:
                self._reserved_servers.add(server_name)
                return server_name, self._configs[executor_class_str][server_name]

        raise ExecutorUnavailable()

    def release_server(self, server_name: str) -> None:
        for executor_class_str, configs in self._configs.items():
            if server_name in configs and server_name in self._reserved_servers:
                self._reserved_servers.remove(server_name)
                self._server_queues[executor_class_str].append(server_name)
                return


class RemoteDockerExecutorManager(BaseExecutorManager):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.remote_servers = RemoteServers()

    async def is_active(self) -> bool:
        return True

    @asynccontextmanager
    async def _connect(self, server_config: _ServerConfig) -> Any:
        async with asyncssh.connect(
            server_config["host"],
            username=server_config["username"],
            client_keys=[server_config["key"]],
            known_hosts=None,
            connect_timeout=15,
            keepalive_interval=30,
        ) as conn:
            yield conn

    async def _connect_no_context(self, server_config: _ServerConfig) -> Any:
        return await asyncssh.connect(
            server_config["host"],
            username=server_config["username"],
            client_keys=[server_config["key"]],
            known_hosts=None,
            connect_timeout=15,
            keepalive_interval=30,
        )

    async def start_new_executor(
        self, token: str, executor_class: ExecutorClass, timeout: float
    ) -> RemoteDockerExecutor:
        if settings.ADDRESS_FOR_EXECUTORS:
            address = settings.ADDRESS_FOR_EXECUTORS
        else:
            raise RuntimeError("ADDRESS_FOR_EXECUTORS is not configured")

        try:
            server_name, server_config = self.remote_servers.reserve_server(str(executor_class))
        except ExecutorUnavailable:
            logger.error("No available servers to reserve")
            raise
        try:
            if not settings.DEBUG_SKIP_PULLING_EXECUTOR_IMAGE:
                async with self._connect(server_config) as conn:
                    process = await conn.create_process("docker pull " + settings.EXECUTOR_IMAGE)
                    try:
                        await asyncio.wait_for(process.wait(), timeout=PULLING_TIMEOUT)
                        if process.returncode:
                            logger.error(
                                f"Pulling executor container failed with returncode={process.returncode}"
                            )
                            raise ExecutorUnavailable("Failed to pull executor image")
                    except TimeoutError:
                        process.kill()
                        logger.error(
                            "Pulling executor container timed out, pulling it from shell might provide more details"
                        )
                        raise ExecutorUnavailable("Failed to pull executor image")

            conn = await self._connect_no_context(server_config)
            args = " ".join(await self.get_executor_cmdline_args())
            process_executor = await conn.create_process(
                f"docker run -e MINER_ADDRESS=ws://{address}:{settings.PORT_FOR_EXECUTORS} "
                f"-e EXECUTOR_TOKEN={token} --name {token} "
                "-v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp "
                f"{settings.EXECUTOR_IMAGE} python manage.py run_executor {args}"
            )
            return RemoteDockerExecutor(process_executor, token, conn, server_name, server_config)
        except Exception:
            self.remote_servers.release_server(server_name)
            raise

    async def kill_executor(self, executor: RemoteDockerExecutor) -> None:
        async with self._connect(executor.server_config) as conn:
            process = await conn.create_process(f"docker stop {executor.token}")
            try:
                await asyncio.wait_for(process.wait(), timeout=DOCKER_STOP_TIMEOUT)
            except TimeoutError:
                pass

            process = await conn.create_process(f"docker stop {executor.token}-job")
            try:
                await asyncio.wait_for(process.wait(), timeout=DOCKER_STOP_TIMEOUT)
            except TimeoutError:
                pass

        try:
            executor.process_executor.kill()
        except OSError:
            pass
        self.remote_servers.release_server(executor.server_name)
        try:
            executor.conn.close()
        except Exception:
            pass

    async def wait_for_executor(self, executor: RemoteDockerExecutor, timeout: float) -> Any:
        try:
            result = await asyncio.wait_for(executor.process_executor.wait(), timeout=timeout)
            if result is not None:
                self.remote_servers.release_server(executor.server_name)
                try:
                    executor.conn.close()
                except Exception:
                    pass
            return result
        except TimeoutError:
            pass

    async def get_manifest(self) -> dict[ExecutorClass, int]:
        manifest = {}
        configs = self.remote_servers.configs
        for executor_class in ExecutorClass:
            executor_class_configs = configs.get(str(executor_class), ())
            if len(executor_class_configs) > 0:
                manifest[executor_class] = len(executor_class_configs)
        return manifest

    async def reserve_executor_class(
        self, token: str, executor_class: ExecutorClass, timeout: float
    ):
        return await super().reserve_executor_class(
            token, executor_class, int(timedelta(minutes=20).total_seconds())
        )

    async def get_executor_class_pool(self, executor_class: ExecutorClass):
        pool = await super().get_executor_class_pool(executor_class)
        pool.set_count(len(self.remote_servers.configs[str(executor_class)]))
        return pool
