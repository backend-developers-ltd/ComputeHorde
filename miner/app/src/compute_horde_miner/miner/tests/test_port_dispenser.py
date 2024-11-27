from unittest.mock import patch

import pytest

from compute_horde_miner.miner.executor_manager.executor_port_dispenser import ExecutorPortDispenser


@pytest.fixture
def dispenser():
    return ExecutorPortDispenser(start_port=8000, end_port=8005)


def test_get_release_port(dispenser):
    # get_port
    port = dispenser.get_port()
    assert port in range(8000, 8005)
    assert port not in dispenser.available_ports
    # release port
    dispenser.release_port(port)
    assert port in dispenser.available_ports


def test_release_port_twice(dispenser):
    port = dispenser.get_port()
    dispenser.release_port(port)
    with pytest.raises(ValueError):
        dispenser.release_port(port)


def test_no_ports_available(dispenser):
    for _ in range(8000, 8005):
        dispenser.get_port()
    with pytest.raises(RuntimeError):
        dispenser.get_port()


def test_is_port_available(dispenser):
    port = dispenser.get_port()
    assert not dispenser.is_port_available(port)
    dispenser.release_port(port)
    assert dispenser.is_port_available(port)


def test_get_port_with_system_check(dispenser):
    with patch("socket.socket.bind", side_effect=OSError("Mocked bind error")) as mock_bind:
        with pytest.raises(RuntimeError):
            dispenser.get_port()
        assert mock_bind.call_count == 5
