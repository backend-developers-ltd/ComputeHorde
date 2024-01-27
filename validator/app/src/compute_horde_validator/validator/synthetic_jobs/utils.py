import asyncio
import datetime

import bittensor
import pydantic
import websockets
from django.conf import settings
from django.utils.timezone import now

from compute_horde.base_requests import BaseRequest
from compute_horde.miner_client.base import AbstractMinerClient, UnsupportedMessageReceived
from compute_horde.mv_protocol import miner_requests, validator_requests
from compute_horde.mv_protocol.miner_requests import BaseMinerRequest
from compute_horde.mv_protocol.validator_requests import V0InitialJobRequest
from validator.app.src.compute_horde_validator.validator.models import Miner, SyntheticJob, SyntheticJobBatch


BASE_DOCKER_IMAGE = ...
DOCKER_IMAGE = ...
JOB_LENGTH = ...
SINGLE_JOB_TIMEOUT = ...
PREPARATION_TIMEOUT = ...

class MinerClient(AbstractMinerClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, miner_address: str, my_hotkey: str, miner_hotkey: str,
                 miner_port: int):
        super().__init__(loop)
        self.miner_hotkey = miner_hotkey
        self.my_hotkey = my_hotkey
        self.miner_address = miner_address
        self.miner_port = miner_port


    def miner_url(self) -> str:
        return (f'wss://{self.miner_address}:{self.miner_port}/'
                    f'v0/validator_interface/{self.my_hotkey}')

    def accepted_request_type(self) -> type[BaseRequest]:
        return BaseMinerRequest

    def incoming_generic_error_class(self):
        return miner_requests.GenericError

    def outgoing_generic_error_class(self):
        return validator_requests.GenericError

    async def handle_message(self, msg: BaseRequest):
        if isinstance(msg, V0InitialJobRequest):
            await self.handle_initial_job_request(msg)
        elif isinstance(msg, V0JobRequest):
            await self.handle_job_request(msg)
        else:
            raise UnsupportedMessageReceived(msg)


def initiate_jobs(netuid, network) -> list[SyntheticJob]:
    metagraph = bittensor.metagraph(netuid, network=network)
    neurons_by_key = {n.hotkey: n for n in metagraph.neurons}
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=now() + datetime.timedelta(seconds=JOB_LENGTH)
    )

    miners = get_miners(metagraph)
    return list(SyntheticJob.objects.bulk_create([
        SyntheticJob(
            batch=batch,
            miner=miner,
            miner_address=neurons_by_key[miner.hotkey].axon_info.ip,
            miner_address_ip_version=neurons_by_key[miner.hotkey].axon_info.ip_type,
            miner_port=neurons_by_key[miner.hotkey].axon_info.port,
            status=SyntheticJob.Status.PENDING
        ) for miner in miners]
    ))


async def execute_job(synthetic_job_id):
    synthetic_job: SyntheticJob = await SyntheticJob.objects.aget(id=synthetic_job_id)
    loop = asyncio.get_event_loop()
    client = MinerClient(
        loop=loop,
        miner_address=synthetic_job.miner_address,
        miner_port=synthetic_job.miner_port,
        miner_hotkey=synthetic_job.miner_hotkey,
        my_hotkey=settings.HOTKEY,
    )
    async with client:
        await client.send_model(V0InitialJobRequest(
            job_uuid=synthetic_job.job_uuid,
            base_docker_image_name=BASE_DOCKER_IMAGE,
            timeout_seconds=
        ))




def get_miners(metagraph) -> list[Miner]:
    existing = Miner.objects.filter(hotkey__in=[n.hotkey for n in metagraph.neurons])
    existing_keys = [m.hotkey for m in existing]
    new_miners = Miner.objects.bulk_create([
        Miner(hotkey=n.hotkey)
        for n in metagraph.neurons if n.hotkey not in existing_keys
    ])
    return existing + new_miners


async def request_synthetic_job(batch_id)
