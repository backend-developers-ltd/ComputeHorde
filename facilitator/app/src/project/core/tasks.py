import contextlib
import csv
import datetime
import io
import shutil
import tempfile
from collections import defaultdict
from datetime import timedelta

import requests
import structlog
from asgiref.sync import async_to_sync
from celery.utils.log import get_task_logger
from channels.layers import get_channel_layer
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    Receipt,
    ReceiptType,
)
from compute_horde_core.executor_class import ExecutorClass
from constance import config
from django.conf import settings
from django.db import connection
from django.utils.timezone import now
from more_itertools import one, partition
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
from .utils import fetch_compute_subnet_hardware, is_validator

log = structlog.wrap_logger(get_task_logger(__name__))

RECEIPTS_CUTOFF_TOLERANCE = timedelta(minutes=30)


@app.task
def sync_metagraph() -> None:
    """Fetch list of current validators from the network and store them in the database"""
    import bittensor

    metagraph = bittensor.metagraph(netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK)
    _, validators = partition(is_validator, metagraph.neurons)

    validators = list(validators)

    our_validator = None
    if our_validator_address := config.OUR_VALIDATOR_SS58_ADDRESS:
        try:
            our_validator = one(
                validator for validator in validators if validator.hotkey == config.OUR_VALIDATOR_SS58_ADDRESS
            )
        except ValueError:
            log.error("our validator not found", our_validator=our_validator_address)

    if limit := config.VALIDATORS_LIMIT:
        validators.sort(key=lambda validator: validator.stake, reverse=True)
        validators = validators[:limit]
        if our_validator and our_validator not in validators:
            try:
                validators[limit - 1] = our_validator
            except IndexError:
                validators.append(our_validator)

    sync_validators.delay([validator.hotkey for validator in validators])
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
    log.debug("validators created", num_created=num_created)


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
    log.debug("miners created", num_created=num_created)


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
def fetch_receipts_from_miner(hotkey: str, ip: str, port: int):
    # Origin of this task may be found in ComputeHorde repo:
    # https://github.com/backend-developers-ltd/ComputeHorde/blob/8b35d24142265171e863e98b3c517ffee007d9a0/compute_horde/compute_horde/receipts.py#L34
    # TODO: reuse receipt fetching code from compute_horde library instead of reimplementing here

    receipts = []
    with contextlib.ExitStack() as exit_stack:
        try:
            receipts_url = f"http://{ip}:{port}/receipts/receipts.csv"
            response = exit_stack.enter_context(requests.get(receipts_url, stream=True, timeout=5))
            response.raise_for_status()
        except RequestException as e:
            log.info("failed to get receipts from miner", miner_hotkey=hotkey, error=e)
            return
        log.info("downloaded receipts from miner", miner_hotkey=hotkey)
        temp_file = exit_stack.enter_context(tempfile.TemporaryFile())
        shutil.copyfileobj(response.raw, temp_file)
        temp_file.seek(0)

        last_receipt_timestamp: datetime.datetime | None = None
        for model in [JobStartedReceipt, JobAcceptedReceipt, JobFinishedReceipt]:
            obj = model.objects.filter(miner_hotkey=hotkey).order_by("-timestamp").first()
            if obj is None:
                continue
            if last_receipt_timestamp is None:
                last_receipt_timestamp = obj.timestamp
            else:
                last_receipt_timestamp = max(last_receipt_timestamp, obj.timestamp)

        if last_receipt_timestamp is not None:
            last_receipt_timestamp -= RECEIPTS_CUTOFF_TOLERANCE

        wrapper = io.TextIOWrapper(temp_file)
        csv_reader = csv.DictReader(wrapper)
        for raw_receipt in csv_reader:
            try:
                timestamp = datetime.datetime.fromisoformat(raw_receipt["timestamp"])
                if last_receipt_timestamp is not None and timestamp <= last_receipt_timestamp:
                    continue

                receipt_type = raw_receipt["type"]
                match receipt_type:
                    case ReceiptType.JobStartedReceipt:
                        payload = JobStartedReceiptPayload(
                            job_uuid=raw_receipt["job_uuid"],
                            miner_hotkey=raw_receipt["miner_hotkey"],
                            validator_hotkey=raw_receipt["validator_hotkey"],
                            timestamp=raw_receipt["timestamp"],
                            executor_class=ExecutorClass(raw_receipt["executor_class"]),
                            max_timeout=int(raw_receipt["max_timeout"]),
                            is_organic=raw_receipt["is_organic"] == "True",
                            ttl=int(raw_receipt["ttl"]),
                        )
                    case ReceiptType.JobAcceptedReceipt:
                        payload = JobAcceptedReceiptPayload(
                            job_uuid=raw_receipt["job_uuid"],
                            miner_hotkey=raw_receipt["miner_hotkey"],
                            validator_hotkey=raw_receipt["validator_hotkey"],
                            timestamp=raw_receipt["timestamp"],
                            time_accepted=raw_receipt["time_accepted"],
                            ttl=int(raw_receipt["ttl"]),
                        )
                    case ReceiptType.JobFinishedReceipt:
                        payload = JobFinishedReceiptPayload(
                            job_uuid=raw_receipt["job_uuid"],
                            miner_hotkey=raw_receipt["miner_hotkey"],
                            validator_hotkey=raw_receipt["validator_hotkey"],
                            timestamp=raw_receipt["timestamp"],
                            time_started=raw_receipt["time_started"],
                            time_took_us=int(raw_receipt["time_took_us"]),
                            score_str=raw_receipt["score_str"],
                        )
                    case unknown_type:
                        log.warning(
                            "Miner sent unknown receipt_type",
                            miner_hotkey=hotkey,
                            receipt_type=unknown_type,
                            raw_receipt=raw_receipt,
                        )
                        continue

                receipt = Receipt(
                    payload=payload,
                    validator_signature=raw_receipt["validator_signature"],
                    miner_signature=raw_receipt["miner_signature"],
                )

            except (ValueError, ValidationError):
                log.warning("Miner sent invalid receipt", miner_hotkey=hotkey, raw_receipt=raw_receipt)
                continue

            if receipt.payload.miner_hotkey != hotkey:
                log.warning("Miner sent receipt of a different miner", miner_hotkey=hotkey, receipt=receipt)
                continue

            if not receipt.verify_miner_signature():
                log.warning("Invalid miner signature of receipt", miner_hotkey=hotkey, receipt=receipt)
                continue

            if not receipt.verify_validator_signature():
                log.warning("Invalid validator signature of receipt", miner_hotkey=hotkey, receipt=receipt)
                continue

            receipts.append(receipt)
    log.info("parsed receipts from miner", miner_hotkey=hotkey, receipts_count=len(receipts))

    job_started_receipt_to_create = [
        JobStartedReceipt(
            job_uuid=receipt.payload.job_uuid,
            validator_hotkey=receipt.payload.validator_hotkey,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_signature=receipt.validator_signature,
            miner_signature=receipt.miner_signature,
            timestamp=receipt.payload.timestamp,
            executor_class=receipt.payload.executor_class,
            max_timeout=receipt.payload.max_timeout,
            is_organic=receipt.payload.is_organic,
            ttl=receipt.payload.ttl,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobStartedReceiptPayload)
    ]
    log.info("start receipts create", miner_hotkey=hotkey, receipts_count=len(job_started_receipt_to_create))
    if job_started_receipt_to_create:
        JobStartedReceipt.objects.bulk_create(job_started_receipt_to_create, ignore_conflicts=True)
    log.info("start receipts created", miner_hotkey=hotkey)

    job_accepted_receipt_to_create = [
        JobAcceptedReceipt(
            job_uuid=receipt.payload.job_uuid,
            validator_hotkey=receipt.payload.validator_hotkey,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_signature=receipt.validator_signature,
            miner_signature=receipt.miner_signature,
            timestamp=receipt.payload.timestamp,
            time_accepted=receipt.payload.time_accepted,
            ttl=receipt.payload.ttl,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobAcceptedReceiptPayload)
    ]
    log.info("accept receipts create", miner_hotkey=hotkey, receipts_count=len(job_accepted_receipt_to_create))
    if job_accepted_receipt_to_create:
        JobAcceptedReceipt.objects.bulk_create(job_accepted_receipt_to_create, ignore_conflicts=True)
    log.info("accept receipts created", miner_hotkey=hotkey)

    job_finished_receipt_to_create = [
        JobFinishedReceipt(
            job_uuid=receipt.payload.job_uuid,
            validator_hotkey=receipt.payload.validator_hotkey,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_signature=receipt.validator_signature,
            miner_signature=receipt.miner_signature,
            timestamp=receipt.payload.timestamp,
            time_started=receipt.payload.time_started,
            time_took_us=receipt.payload.time_took_us,
            score_str=receipt.payload.score_str,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobFinishedReceiptPayload)
    ]
    log.info("finish receipts create", miner_hotkey=hotkey, receipts_count=len(job_finished_receipt_to_create))
    if job_finished_receipt_to_create:
        JobFinishedReceipt.objects.bulk_create(job_finished_receipt_to_create, ignore_conflicts=True)
    log.info("finish receipts created", miner_hotkey=hotkey)


@app.task
def fetch_receipts():
    """Fetch job receipts from the miners."""
    import bittensor

    metagraph = bittensor.metagraph(netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK)
    miners = [neuron for neuron in metagraph.neurons if neuron.axon_info.is_serving]
    for miner in miners:
        fetch_receipts_from_miner.delay(miner.hotkey, miner.axon_info.ip, miner.axon_info.port)


@app.task
def refresh_specs_materialized_view():
    log.info("Refreshing specs materialized view")
    with connection.cursor() as cursor:
        cursor.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY specs")


@app.task
def evict_old_data():
    eviction.evict_all()
