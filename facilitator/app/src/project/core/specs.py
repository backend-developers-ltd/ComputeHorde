from collections import defaultdict

import structlog
from celery.utils.log import get_task_logger
from compute_horde.fv_protocol.validator_requests import V0MachineSpecsUpdate
from django.utils.timezone import now
from pydantic import parse_obj_as

from .models import (
    GPU,
    CpuSpecs,
    ExecutorSpecsSnapshot,
    GpuSpecs,
    Miner,
    OtherSpecs,
    ParsedSpecsData,
    RawSpecsData,
    Validator,
)
from .schemas import HardwareSpec

log = structlog.wrap_logger(get_task_logger(__name__))

SPECS_PROCESS_LOOKBACK = 60 * 65  # 65 minutes


def normalize_gpu_name(name: str) -> str:
    return name.upper().replace("-", " ").replace("NVIDIA", " ").strip()


def cleanup_fields(raw_specs):
    # remove variable fields
    try:
        if "free" in raw_specs["ram"]:
            del raw_specs["ram"]["free"]
        if "available" in raw_specs["ram"]:
            del raw_specs["ram"]["available"]
        if "used" in raw_specs["ram"]:
            del raw_specs["ram"]["used"]
        if "free" in raw_specs["hard_disk"]:
            del raw_specs["hard_disk"]["free"]
        if "available" in raw_specs["hard_disk"]:
            del raw_specs["hard_disk"]["available"]
        if "used" in raw_specs["hard_disk"]:
            del raw_specs["hard_disk"]["used"]
    except KeyError:
        log.error("could not remove variable ram and hard disk fields from: {raw_specs}")


async def save_machine_specs(message: V0MachineSpecsUpdate) -> None:
    message = message.dict()
    raw_specs = message["specs"]
    validator_hotkey = message["validator_hotkey"]
    miner_hotkey = message["miner_hotkey"]
    batch_id = message.get("batch_id", None)
    log.debug(f"received miner {miner_hotkey} specs {raw_specs} from validator {validator_hotkey}, batch_id {batch_id}")
    measured_at = now()

    miner = await Miner.objects.filter(ss58_address=miner_hotkey, is_active=True).afirst()
    if not miner:
        log.warning(f"miner with hotkey {miner_hotkey} not found - not storing hardware state")
        return

    validator = await Validator.objects.filter(ss58_address=validator_hotkey, is_active=True).afirst()
    if not validator:
        log.warning(f"validator with hotkey {miner_hotkey} not found - not storing hardware state")
        return

    cleanup_fields(raw_specs)

    # dump raw specs data
    raw_specs, _ = await RawSpecsData.objects.aget_or_create(
        data=raw_specs,
    )

    await ExecutorSpecsSnapshot.objects.acreate(
        batch_id=batch_id,
        miner=miner,
        validator=validator,
        measured_at=measured_at,
        raw_specs=raw_specs,
    )

    raw_specs_is_parsed = await ParsedSpecsData.objects.filter(pk=raw_specs.pk).aexists()
    if not raw_specs_is_parsed:
        await process_raw_specs_data(raw_specs)
        log.info(
            f"processed new specs data from miner {miner_hotkey}, validator {validator_hotkey}, batch {batch_id} measured at {measured_at}"
        )


async def process_raw_specs_data(raw_specs: RawSpecsData) -> None:
    log.info(f"Trigger running process_raw_specs_data for {raw_specs}")

    try:
        specs = parse_obj_as(HardwareSpec, raw_specs.data)
    except Exception as e:
        log.error(f"failed to parse raw specs data: {raw_specs.data}: {e}")

    if specs.model_extra:
        log.warning(f"extra fields found in raw specs: {specs.model_extra}")

    cpu_specs, _ = await CpuSpecs.objects.aget_or_create(
        cpu_model=specs.cpu.model,
        cpu_count=specs.cpu.count,
    )

    other_specs, _ = await OtherSpecs.objects.aget_or_create(
        os=specs.os,
        virtualization=specs.virtualization,
        total_ram=specs.ram.get_total_gb(),
        total_hdd=specs.hard_disk.get_total_gb(),
    )

    parsed_specs, is_created = await ParsedSpecsData.objects.aget_or_create(
        id=raw_specs,
        defaults={
            "cpu_specs": cpu_specs,
            "other_specs": other_specs,
        },
    )

    # if parsed_specs already exists, then gpu_specs should also exist
    if is_created:
        gpu_types_count = defaultdict(int)
        gpu_types_details = {}

        if specs.gpu is None:
            log.error(f"no gpu details found in: {specs}")
            return

        for gpu_detail in specs.gpu.details:
            gpu_name = normalize_gpu_name(gpu_detail.name)
            gpu_types_count[gpu_name] += 1
            gpu_types_details[gpu_name] = gpu_detail

        instances = []
        for gpu_name, gpu_detail in gpu_types_details.items():
            # try to match with reference gpu, otherwise create new
            gpu, is_created = await GPU.objects.aget_or_create(name=gpu_name)
            if is_created:
                log.warning(f"gpu: {gpu_name} not found in db - created new entry")

            instances.append(
                GpuSpecs(
                    parsed_specs=parsed_specs,
                    gpu_model=gpu,
                    gpu_count=gpu_types_count[gpu_name],
                    capacity=gpu_detail.capacity,
                    cuda=gpu_detail.cuda,
                    driver=gpu_detail.driver,
                    graphics_speed=gpu_detail.graphics_speed,
                    memory_speed=gpu_detail.memory_speed,
                    power_limit=gpu_detail.power_limit,
                    uuid=gpu_detail.uuid,
                    serial=gpu_detail.serial,
                )
            )
        await GpuSpecs.objects.abulk_create(instances)
