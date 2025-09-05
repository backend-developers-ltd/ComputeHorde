import uuid
from pathlib import Path
from textwrap import dedent

import asyncssh
import pytest
import pytest_asyncio
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_miner.miner.executor_manager._internal.docker import (
    DockerExecutorManager,
)

# NOTE: these tests use the local docker to run containers.


@pytest.mark.asyncio
@pytest.mark.django_db
@pytest.mark.parametrize(
    "executor_class",
    [executor_class for executor_class in ExecutorClass],
)
async def test_docker_executor_manager_default(settings, executor_class):
    settings.DEFAULT_EXECUTOR_CLASS = str(executor_class)
    settings.ADDRESS_FOR_EXECUTORS = "127.0.0.1"
    settings.DEBUG_AUTO_REMOVE_EXECUTOR_CONTAINERS = True
    settings.EXECUTOR_IMAGE = "python:3.11-slim"

    executor_manager = DockerExecutorManager()

    # Run twice to check if the single default executor is released correctly.
    for _ in range(2):
        token = f"unittest-docker-{uuid.uuid4()}"
        executor = await executor_manager.reserve_executor_class(token, executor_class, 10)
        assert executor.token == token
        assert executor.server_name == "self"
        assert executor.server_config.executor_class == executor_class

        await executor_manager.wait_for_executor_reservation(token, executor_class)

        exit_code = await executor_manager.wait_for_executor(executor, 10)
        assert exit_code is not None


class LocalProxySSHServer(asyncssh.SSHServer):
    def begin_auth(self, username):
        # Returning False means "no authentication required"
        return False

    def unix_connection_requested(self, dest_path):
        return dest_path == "/var/run/docker.sock"


@pytest_asyncio.fixture
async def local_proxy_ssh_server():
    """
    A local SSH server that accepts port forwards to /var/run/docker.sock.
    Returns the port number of the server.
    """
    host_key = asyncssh.generate_private_key("ssh-ed25519")

    async with asyncssh.listen(
        host="127.0.0.1",
        port=0,
        server_factory=LocalProxySSHServer,
        server_host_keys=[host_key],
    ) as server:
        yield server.get_port()


@pytest_asyncio.fixture
async def config_path(tmp_path: Path, local_proxy_ssh_server):
    key_path = tmp_path / "id_ed25519"
    key_path.write_text(asyncssh.generate_private_key("ssh-ed25519").export_private_key().decode())

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        dedent(f"""
            remote-executor:
              executor_class: always_on.llm.a6000
              mode: ssh
              host: "127.0.0.1"
              ssh_port: {local_proxy_ssh_server}
              username: "nobody"
              key_path: {key_path.as_posix()}
            """)
    )
    return config_path.as_posix()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_docker_executor_manager_ssh_tunnel(settings, config_path):
    settings.ADDRESS_FOR_EXECUTORS = "127.0.0.1"
    settings.DOCKER_EXECUTORS_CONFIG_PATH = config_path
    settings.DEBUG_AUTO_REMOVE_EXECUTOR_CONTAINERS = True
    settings.EXECUTOR_IMAGE = "python:3.11-slim"

    executor_manager = DockerExecutorManager()

    # Run twice to check if the single remote executor is released correctly.
    for _ in range(2):
        token = f"unittest-docker-{uuid.uuid4()}"
        executor = await executor_manager.reserve_executor_class(
            token, ExecutorClass.always_on__llm__a6000, 10
        )
        assert executor.token == token
        assert executor.server_name == "remote-executor"
        assert executor.server_config.executor_class == ExecutorClass.always_on__llm__a6000

        await executor_manager.wait_for_executor_reservation(
            token, ExecutorClass.always_on__llm__a6000
        )

        exit_code = await executor_manager.wait_for_executor(executor, 10)
        assert exit_code is not None
