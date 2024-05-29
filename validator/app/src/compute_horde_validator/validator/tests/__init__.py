"""
This file is required by pytest, otherwise import errors will pop up:

project/core/tests/conftest.py:8: in <module>
    from .models import User
E   ImportError: attempted relative import with no known parent package
"""

import asyncio

import bittensor
import pytest


def mock_keypair():
    return bittensor.Keypair.create_from_mnemonic(
        mnemonic="arrive produce someone view end scout bargain coil slight festival excess struggle"
    )


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
