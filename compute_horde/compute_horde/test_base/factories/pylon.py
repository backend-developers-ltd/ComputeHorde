from ipaddress import IPv4Address
from typing import Any

import pytest
from polyfactory import Use
from polyfactory.factories.pydantic_factory import ModelFactory
from pylon_client.v1 import AxonInfo, AxonProtocol, Neuron, Stakes


class ResetableSequence:
    def __init__(self, start: int = 1):
        self.start = start
        self.counter = start

    def __iter__(self):
        return self

    def __next__(self):
        val = self.counter
        self.counter += 1
        return val

    def reset(self):
        self.counter = self.start


uid_sequence = ResetableSequence()


class NeuronFactory(ModelFactory[Neuron]):
    __model__ = Neuron

    @classmethod
    def axon_info(cls) -> AxonInfo:
        return AxonInfo(
            ip=IPv4Address("127.0.0.1"),
            port=8000,
            protocol=AxonProtocol.TCP,
        )

    @classmethod
    def stakes(cls) -> Stakes:
        return Stakes(alpha=0, tao=0, total=0)

    uid = Use(lambda: next(uid_sequence))


class PylonFactory:
    """Helper container to provide a clean API for neuron creation."""

    @staticmethod
    def neuron(**kwargs: Any) -> Neuron:
        if "total_stake" in kwargs:
            total = kwargs.pop("total_stake")
            kwargs.setdefault("stakes", Stakes(alpha=0, tao=0, total=total))
            kwargs.setdefault("stake", total)

        if "axon_info_is_serving" in kwargs:
            is_serving = kwargs.pop("axon_info_is_serving")
            ip = "127.0.0.1" if is_serving else "0.0.0.0"
            kwargs.setdefault("axon_info", AxonInfo(
                ip=IPv4Address(ip),
                port=8000,
                protocol=AxonProtocol.TCP
            ))

        return NeuronFactory.build(**kwargs)

    @staticmethod
    def reset() -> None:
        """Reset sequences for UID generation."""
        uid_sequence.reset()


@pytest.fixture(autouse=True)
def reset_pylon_factory():
    """Automatically reset the UID sequence before each test case."""
    PylonFactory.reset()
