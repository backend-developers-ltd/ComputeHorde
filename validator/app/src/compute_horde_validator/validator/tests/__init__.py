"""
This file is required by pytest, otherwise import errors will pop up:

project/core/tests/conftest.py:8: in <module>
    from .models import User
E   ImportError: attempted relative import with no known parent package
"""

import asyncio
from typing import NamedTuple

import bittensor
from asgiref.sync import sync_to_async
from django.conf import settings


def throw_error(*args):
    raise Exception("Error thrown for testing")


def mock_keypair():
    return bittensor.Keypair.create_from_mnemonic(
        mnemonic="arrive produce someone view end scout bargain coil slight festival excess struggle"
    )


class MockWallet:
    def __init__(self, *args):
        pass

    def get_hotkey(self):
        return mock_keypair()


def get_miner_client(MINER_CLIENT, job_uuid: str):
    return MINER_CLIENT(
        loop=asyncio.get_event_loop(),
        miner_address="ignore",
        my_hotkey="ignore",
        miner_hotkey="ignore",
        miner_port=9999,
        job_uuid=job_uuid,
        keypair=mock_keypair(),
    )


class MockedAxonInfo(NamedTuple):
    is_serving: bool
    ip: str
    ip_type: int
    port: int


async def mock_get_miner_axon_info(hotkey: str):
    return MockedAxonInfo(is_serving=True, ip_type=4, ip="0000", port=8000)
