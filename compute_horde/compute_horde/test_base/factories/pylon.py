from ipaddress import IPv4Address, IPv6Address
from typing import Any, TypeAlias

import pytest
from polyfactory import Use
from polyfactory.factories.pydantic_factory import ModelFactory
from pylon_client.v1 import AxonInfo, AxonProtocol, Neuron, Stakes


class Undefined:
    pass


undefined: Any = Undefined()


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
    __random_seed__ = 42

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
    def neuron(
        uid: int | Undefined = undefined,
        coldkey: str | Undefined = undefined,
        hotkey: str | Undefined = undefined,
        active: bool | Undefined = undefined,
        stake: int | float | Undefined = undefined,
        rank: int | float | Undefined = undefined,
        emission: int | float | Undefined = undefined,
        incentive: int | float | Undefined = undefined,
        consensus: int | float | Undefined = undefined,
        trust: int | float | Undefined = undefined,
        validator_trust: int | float | Undefined = undefined,
        dividends: int | float | Undefined = undefined,
        last_update: int | Undefined = undefined,
        validator_permit: bool | Undefined = undefined,
        pruning_score: int | Undefined = undefined,
        # AxonInfo fields
        axon_info_ip: IPv4Address | IPv6Address | str | Undefined = undefined,
        axon_info_port: int | Undefined = undefined,
        axon_info_protocol: AxonProtocol | Undefined = undefined,
        # Stakes fields
        stakes_alpha: int | float | Undefined = undefined,
        stakes_tao: int | float | Undefined = undefined,
        stakes_total: int | float | Undefined = undefined,
    ) -> Neuron:
        kwargs = {}
        if uid is not undefined:
            kwargs["uid"] = uid
        if coldkey is not undefined:
            kwargs["coldkey"] = coldkey
        if hotkey is not undefined:
            kwargs["hotkey"] = hotkey
        if active is not undefined:
            kwargs["active"] = active
        if stake is not undefined:
            kwargs["stake"] = stake
        if rank is not undefined:
            kwargs["rank"] = rank
        if emission is not undefined:
            kwargs["emission"] = emission
        if incentive is not undefined:
            kwargs["incentive"] = incentive
        if consensus is not undefined:
            kwargs["consensus"] = consensus
        if trust is not undefined:
            kwargs["trust"] = trust
        if validator_trust is not undefined:
            kwargs["validator_trust"] = validator_trust
        if dividends is not undefined:
            kwargs["dividends"] = dividends
        if last_update is not undefined:
            kwargs["last_update"] = last_update
        if validator_permit is not undefined:
            kwargs["validator_permit"] = validator_permit
        if pruning_score is not undefined:
            kwargs["pruning_score"] = pruning_score
        kwargs["stakes"] = Stakes(
            alpha=stakes_alpha if stakes_alpha is not undefined else 0,
            tao=stakes_tao if stakes_tao is not undefined else 0,
            total=stakes_total if stakes_total is not undefined else 0,
        )
        kwargs["axon_info"] = AxonInfo(
            ip=IPv4Address("127.0.0.1") if axon_info_ip is undefined else axon_info_ip,
            port=8000 if axon_info_port is undefined else axon_info_port,
            protocol=AxonProtocol.TCP if axon_info_protocol is undefined else axon_info_protocol,
        )

        return NeuronFactory.build(**kwargs)

    @staticmethod
    def validator(
            uid: int | Undefined = undefined,
            coldkey: str | Undefined = undefined,
            hotkey: str | Undefined = undefined,
            active: bool | Undefined = undefined,
            axon_info: AxonInfo | Undefined = undefined,
            stake: int | float | Undefined = undefined,
            rank: int | float | Undefined = undefined,
            emission: int | float | Undefined = undefined,
            incentive: int | float | Undefined = undefined,
            consensus: int | float | Undefined = undefined,
            trust: int | float | Undefined = undefined,
            validator_trust: int | float | Undefined = undefined,
            dividends: int | float | Undefined = undefined,
            last_update: int | Undefined = undefined,
            validator_permit: bool | Undefined = True,
            pruning_score: int | Undefined = undefined,
            # AxonInfo fields
            axon_info_ip: IPv4Address | IPv6Address | str | Undefined = IPv4Address("0.0.0.0"),
            axon_info_port: int | Undefined = 0,
            axon_info_protocol: AxonProtocol | Undefined = undefined,
            # Stakes fields
            stakes_alpha: int | float | Undefined = int(1e4),
            stakes_tao: int | float | Undefined = int(1e4),
            stakes_total: int | float | Undefined = 2 * int(1e4),

    ) -> Neuron:
        return PylonFactory.neuron(*args, validator_permit=True, **kwargs)

    @staticmethod
    def reset() -> None:
        """Reset sequences for UID generation."""
        uid_sequence.reset()
        NeuronFactory.seed_random(42)


@pytest.fixture(autouse=True)
def reset_pylon_factory():
    """Automatically reset the UID sequence before each test case."""
    PylonFactory.reset()
