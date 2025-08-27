from datetime import timedelta
from pathlib import Path
from textwrap import dedent

import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_miner.miner.executor_manager._internal.docker import (
    DockerExecutorConfigError,
    ServerManager,
)


def test_server_manager__default_path_should_not_raise() -> None:
    assert ServerManager("__default__").fetch_config()


def test_server_manager__invalid_path_should_raise(tmp_path: Path) -> None:
    non_existent = tmp_path / "config.yaml"
    with pytest.raises(DockerExecutorConfigError):
        ServerManager(non_existent.as_posix())


def test_server_manager__non_file_path_should_raise(tmp_path: Path) -> None:
    non_file = tmp_path / "config"
    non_file.mkdir()
    with pytest.raises(DockerExecutorConfigError):
        ServerManager(non_file.as_posix())


def test_server_manager__invalid_config_should_raise(tmp_path: Path) -> None:
    invalid = tmp_path / "config.yaml"
    invalid.write_text(
        dedent("""
            s1:
            s2:
            """)
    )
    server_manager = ServerManager(invalid.as_posix())
    with pytest.raises(DockerExecutorConfigError):
        server_manager.fetch_config()


def test_server_manager_roundrobin_queue(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        dedent("""
            a:
              executor_class: always_on.llm.a6000
              host: "1.2.3.4"
              ssh_port: 22
              username: "user"
              key_path: "/path/to/key"
            b:
              executor_class: always_on.llm.a6000
              host: "1.2.3.4"
              ssh_port: 22
              username: "user"
              key_path: "/path/to/key"
            c:
              executor_class: always_on.llm.a6000
              host: "1.2.3.4"
              ssh_port: 22
              username: "user"
              key_path: "/path/to/key"
            """)
    )

    server_manager = ServerManager(config_path.as_posix())
    got_servers = []
    for _ in range(6):
        server = server_manager.reserve_server(ExecutorClass.always_on__llm__a6000)
        got_servers.append(server.name)
        server_manager.release_server(server)

    assert got_servers == ["a", "b", "c", "a", "b", "c"]


def test_server_manager_queues_are_updated_from_config(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        dedent("""
            a:
              executor_class: always_on.llm.a6000
              host: "1.2.3.4"
              ssh_port: 22
              username: "user"
              key_path: "/path/to/key"
            """)
    )

    server_manager = ServerManager(config_path.as_posix(), cache_duration=timedelta(0))

    got_servers_1 = []
    for _ in range(3):
        server = server_manager.reserve_server(ExecutorClass.always_on__llm__a6000)
        got_servers_1.append(server.name)
        server_manager.release_server(server)
    assert set(got_servers_1) == {"a"}

    # remove old server, add new server
    config_path.write_text(
        dedent("""
            b:
              executor_class: always_on.llm.a6000
              host: "1.2.3.4"
              ssh_port: 22
              username: "user"
              key_path: "/path/to/key"
            """)
    )

    got_servers_2 = []
    for _ in range(3):
        server = server_manager.reserve_server(ExecutorClass.always_on__llm__a6000)
        got_servers_2.append(server.name)
        server_manager.release_server(server)
    assert set(got_servers_2) == {"b"}
