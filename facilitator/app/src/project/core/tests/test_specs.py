from uuid import uuid4

import pytest
from compute_horde.fv_protocol.validator_requests import V0MachineSpecsUpdate

from project.core.specs import process_raw_specs_data, save_machine_specs

from ..models import (
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

dummy_specs: dict = {
    "os": "Ubuntu Noble Numbat",
    "cpu": {"count": 48, "model": "Intel(R) Xeon(R) CPU @ 2.20GHz", "clocks": [2200.218, 2200.218, 2200.218, 2200.218]},
    "gpu": {
        "count": 3,
        "details": [
            {
                "cuda": "9.0",
                "name": "NVIDIA 5090x",
                "driver": "551.54.15",
                "capacity": 8998.98,
                "power_limit": "800.00",
                "memory_speed": 1215,
                "graphics_speed": 210,
                "uuid": "GPU-56684bba-5239-734e-1e4d-53e72e8dccd1",
                "serial": "1323122012526",
            },
            {
                "cuda": "8.0",
                "name": "NVIDIA A100-SXM4-40GB",
                "driver": "550.54.15",
                "capacity": 40960,
                "power_limit": "N/A",
                "memory_speed": 1215,
                "graphics_speed": 210,
                "uuid": "GPU-1f1c4420-5239-734e-1e4d-53e72e8dccd1",
                "serial": "[N/A]",
            },
            {
                "cuda": "8.0",
                "name": "NVIDIA A100-SXM4-40GB",
                "driver": "550.54.15",
                "capacity": "40960",
                "power_limit": 400.00,
                "memory_speed": 1215,
                "graphics_speed": 210,
            },
        ],
    },
    "ram": {"free": 279004224, "used": 71605036, "total": "71605036.00019", "available": 338468088},
    "hard_disk": {"free": 169588304, "used": 84264672, "total": 253869360},
    "virtualization": "kvm\ndocker",
}


async def setup_db():
    await Validator.objects.acreate(ss58_address="validator_hotkey", is_active=True)
    await Miner.objects.acreate(ss58_address="miner_hotkey", is_active=True)


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_machine_specs_udpate():
    await setup_db()
    assert await ParsedSpecsData.objects.acount() == 0

    message = V0MachineSpecsUpdate(
        specs=dummy_specs, miner_hotkey="miner_hotkey", validator_hotkey="validator_hotkey", batch_id=str(uuid4())
    )
    await save_machine_specs(message)

    assert await RawSpecsData.objects.acount() == 1
    assert await ExecutorSpecsSnapshot.objects.acount() == 1
    assert await ParsedSpecsData.objects.acount() == 1

    # add record with same raw_specs
    await save_machine_specs(message)
    await save_machine_specs(message)

    assert await RawSpecsData.objects.acount() == 1
    assert await ExecutorSpecsSnapshot.objects.acount() == 3
    assert await ParsedSpecsData.objects.acount() == 1


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_process_raw_specs_data():
    await setup_db()
    raw_specs_data = await RawSpecsData.objects.acreate(data=dummy_specs)
    await process_raw_specs_data(raw_specs_data)

    assert await CpuSpecs.objects.acount() == 1
    assert await OtherSpecs.objects.acount() == 1
    assert await ParsedSpecsData.objects.acount() == 1

    other_specs = await OtherSpecs.objects.afirst()
    other_specs.free_ram = 279004224
    other_specs.total_ram = 3506

    num_gpu_specs = await GpuSpecs.objects.acount()
    assert num_gpu_specs == 2

    gpu_model = await GPU.objects.filter(name="5090X").afirst()
    gpu_spec = await GpuSpecs.objects.filter(gpu_model=gpu_model).afirst()
    assert gpu_spec.capacity == 8998
    assert gpu_spec.gpu_count == 1
    assert gpu_spec.serial == "1323122012526"
    assert gpu_spec.uuid == "GPU-56684bba-5239-734e-1e4d-53e72e8dccd1"

    gpu_model = await GPU.objects.filter(name="A100 SXM4 40GB").afirst()
    gpu_spec = await GpuSpecs.objects.filter(gpu_model=gpu_model).afirst()
    assert gpu_spec.capacity == 40960
    assert gpu_spec.gpu_count == 2
