import abc
import contextlib
import enum
import time
from typing import Any, assert_never

import numpy as np
import requests
from django.conf import settings


class PylonClientError(Exception):
    def __init__(self, reason: Exception):
        self.reason = reason
        super().__init__(reason)


class AbstractPylonClient:
    @abc.abstractmethod
    def set_weights(self, weights: dict[str, int | float | np.floating[Any]]): ...


class DefaultPylonClient(AbstractPylonClient):
    def set_weights(self, weights: dict[str, int | float | np.floating[Any]]):
        # in case there is some numpy madness:
        weights = {hk: float(weight) for hk, weight in weights.items()}
        try:
            resp = requests.put(
                f"http://{settings.PYLON_HOST}:{settings.PYLON_PORT}/subnet/weights",
                json={"weights": weights},
                headers={"Authorization": f"Bearer {settings.PYLON_AUTH_TOKEN}"},
                timeout=10,
            )
            resp.raise_for_status()
        except Exception as e:
            raise PylonClientError(e) from e


class MockPylonBehaviour(enum.Enum):
    raise_ = "raise"
    sleep_and_raise = "sleep_and_raise"
    work_fine_forever = "work_fine_forever"


class MockPylonClient(AbstractPylonClient):
    def __init__(self, behaviour: None | list[MockPylonBehaviour] = None):
        if behaviour is None:
            self.behaviour = [MockPylonBehaviour.work_fine_forever]
        else:
            self.behaviour = behaviour
        self.weights_submitted: list[dict[str, float]] = []

    def set_weights(self, weights: dict[str, int | float | np.floating[Any]]):
        # in case there is some numpy madness:
        self.weights_submitted.append({hk: float(weight) for hk, weight in weights.items()})
        if not self.behaviour:
            return

        if self.behaviour[0] == MockPylonBehaviour.work_fine_forever:
            return
        elif self.behaviour[0] == MockPylonBehaviour.raise_:
            self.behaviour.pop(0)
            raise PylonClientError(Exception("MockPylonClient error"))
        elif self.behaviour[0] == MockPylonBehaviour.sleep_and_raise:
            self.behaviour.pop(0)
            time.sleep(5)
            raise PylonClientError(Exception("MockPylonClient error"))
        else:
            assert_never(self.behaviour[0])


_pylon_client_instance: AbstractPylonClient | None = None


def pylon_client() -> AbstractPylonClient:
    global _pylon_client_instance
    if _pylon_client_instance is None:
        _pylon_client_instance = DefaultPylonClient()
    return _pylon_client_instance


@contextlib.contextmanager
def setup_mock_pylon_client(behaviour: None | list[MockPylonBehaviour] = None):
    global _pylon_client_instance
    prev_pylon_client_instance = _pylon_client_instance

    _pylon_client_instance = MockPylonClient(behaviour)
    yield _pylon_client_instance
    _pylon_client_instance = prev_pylon_client_instance
