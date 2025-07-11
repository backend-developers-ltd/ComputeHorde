from collections import defaultdict
from datetime import timedelta

import requests
import structlog
from asgiref.sync import async_to_sync
from celery.utils.log import get_task_logger
from channels.layers import get_channel_layer
from compute_horde.utils import get_validators
from django.conf import settings
from django.db import connection
from django.utils.timezone import now
from pydantic import BaseModel, ValidationError, parse_obj_as
from requests import RequestException

from project.celery import app

from . import eviction
from .models import (
    GPU,
    Channel,
    GpuCount,
    HardwareState,
    Miner,
    Subnet,
    Validator,
)
from .models import MinerVersion as MinerVersionDTO
from .schemas import ForceDisconnect, HardwareSpec
from .specs import normalize_gpu_name
from .utils import fetch_compute_subnet_hardware

log = structlog.wrap_logger(get_task_logger(__name__))

RECEIPTS_CUTOFF_TOLERANCE = timedelta(minutes=30)


@app.task
def sync_metagraph() -> None:
    """Fetch current validators and miners from the network and store them in the database"""
    import bittensor

    with bittensor.subtensor(network=settings.BITTENSOR_NETWORK) as subtensor:
        metagraph = subtensor.metagraph(netuid=settings.BITTENSOR_NETUID)
        validators = get_validators(metagraph=metagraph)

    sync_validators.delay([v.hotkey for v in validators])
    sync_miners.delay([neuron.hotkey for neuron in metagraph.neurons if neuron.axon_info.is_serving])


@app.task
def sync_validators(active_validators_keys: list[str]) -> None:
    to_deactivate = Validator.objects.filter(is_active=True).exclude(ss58_address__in=active_validators_keys)
    to_deactivate_ids = list(to_deactivate.values_list("id", flat=True))
    num_deactivated = to_deactivate.update(is_active=False)
    log.debug("validators deactivated", num_deactivated=num_deactivated)

    channel_layer = get_channel_layer()
    send = async_to_sync(channel_layer.send)
    for channel in Channel.objects.filter(validator__in=to_deactivate_ids).select_related("validator"):
        log.debug("disconnecting channel", validator=channel.validator, channel=channel)
        send(channel.name, ForceDisconnect().dict())

    for channel in Channel.objects.filter(last_heartbeat__lte=now() - timedelta(minutes=10)).select_related(
        "validator"
    ):
        log.debug("disconnecting channel", validator=channel.validator, channel=channel)
        send(channel.name, ForceDisconnect().dict())

    Channel.objects.filter(last_heartbeat__lte=now() - timedelta(minutes=10)).delete()

    to_activate = Validator.objects.filter(is_active=False, ss58_address__in=active_validators_keys)
    num_activated = to_activate.update(is_active=True)
    log.debug("validators activated", num_activated=num_activated)

    to_create = set(active_validators_keys) - set(Validator.objects.values_list("ss58_address", flat=True))
    num_created = Validator.objects.bulk_create(
        [Validator(ss58_address=ss58_address, is_active=True) for ss58_address in to_create]
    )
    log.debug("validators created", num_created=len(num_created))


@app.task
def sync_miners(active_miners_keys: list[str]) -> None:
    to_deactivate = Miner.objects.filter(is_active=True).exclude(ss58_address__in=active_miners_keys)
    num_deactivated = to_deactivate.update(is_active=False)
    log.debug("miners deactivated", num_deactivated=num_deactivated)

    to_activate = Miner.objects.filter(is_active=False, ss58_address__in=active_miners_keys)
    num_activated = to_activate.update(is_active=True)
    log.debug("miners activated", num_activated=num_activated)

    to_create = set(active_miners_keys) - set(Miner.objects.values_list("ss58_address", flat=True))
    num_created = Miner.objects.bulk_create(
        [Miner(ss58_address=ss58_address, is_active=True) for ss58_address in to_create]
    )
    log.debug("miners created", num_created=len(num_created))


@app.task
def record_compute_subnet_hardware() -> None:
    """
    Fetch hardware info from compute subnet and store it in the database.

    Two main "items" are stored:
    1) HardwareState: raw hardware specs as received from the subnet (json)
    2) GpuCount: count of each GPU model present in the subnet at this moment
    """

    raw_specs = fetch_compute_subnet_hardware()
    log.debug("compute subnet hardware", raw_specs=raw_specs)

    now_ = now()
    compute_subnet, _ = Subnet.objects.get_or_create(
        uid=settings.COMPUTE_SUBNET_UID,
        defaults={
            "name": "Compute",
        },
    )

    HardwareState.objects.create(
        subnet=compute_subnet,
        state=raw_specs,
        measured_at=now_,
    )
    log.info("raw hardware state stored")

    log.debug("parsing hardware specs")
    specs = parse_obj_as(dict[str, HardwareSpec], raw_specs)

    count = defaultdict(int)
    for hardware_specs in specs.values():
        for detail in hardware_specs.gpu.details:
            count[normalize_gpu_name(detail.name)] += 1

    instances = [
        GpuCount(
            subnet=compute_subnet,
            gpu=GPU.objects.get_or_create(name=gpu_name)[0],
            count=num,
            measured_at=now_,
        )
        for gpu_name, num in count.items()
    ]
    GpuCount.objects.bulk_create(instances)
    log.info("gpu counts stored")


class MinerVersion(BaseModel):
    miner_version: str
    runner_version: str


@app.task
def fetch_miner_version(hotkey: str, ip: str, port: int) -> None:
    """
    Fetch miner & miner runner versions for a given miner by scraping its `/version` endpoint.
    """
    try:
        r = requests.get(f"http://{ip}:{port}/version", timeout=1)
        r.raise_for_status()
        version = MinerVersion.parse_raw(r.content)
    except (RequestException, ValidationError):
        log.debug("miner version fetching failed", miner_hotkey=hotkey, miner_ip=ip, miner_port=port)
        version = MinerVersion(miner_version="", runner_version="")

    try:
        miner = Miner.objects.get(ss58_address=hotkey)
    except Miner.DoesNotExist:
        # on the next run of sync_metagraph(), the miner will in in db.
        log.debug("recording miner version failed, miner does not exist in db", miner_hotkey=hotkey, version=version)
        return

    # don't insert if the last recorded versions are same
    last_recorded_version = MinerVersionDTO.objects.filter(miner=miner).order_by("-created_at").first()
    if (
        last_recorded_version is not None
        and last_recorded_version.version == version.miner_version
        and last_recorded_version.runner_version == version.runner_version
    ):
        log.debug("miner version is same as last recorded version", miner_hotkey=hotkey, version=version)
    else:
        MinerVersionDTO.objects.create(
            miner=miner,
            version=version.miner_version,
            runner_version=version.runner_version,
        )


@app.task
def fetch_miner_versions() -> None:
    """
    Fetch miner & miner runner versions for every active miner.
    The list of active miners is retrieved from the metagraph.
    """
    import bittensor

    metagraph = bittensor.metagraph(netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK)
    miners = [neuron for neuron in metagraph.neurons if neuron.axon_info.is_serving]
    for miner in miners:
        fetch_miner_version.delay(miner.hotkey, miner.axon_info.ip, miner.axon_info.port)


@app.task
def refresh_specs_materialized_view():
    log.info("Refreshing specs materialized view")
    with connection.cursor() as cursor:
        cursor.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY specs")


@app.task
def evict_old_data():
    eviction.evict_all()
