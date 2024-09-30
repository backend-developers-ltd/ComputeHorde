from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum


class ExecutorClass(StrEnum):
    spin_up_4min__gpu_24gb = "spin_up-4min.gpu-24gb"
    always_on__gpu_24gb = "always_on.gpu-24gb"
    always_on__llm__a6000 = "always_on.llm.a6000"
    # always_on__cpu_16c__ram_64gb = "always_on.cpu-16c.ram-64gb"
    # always_on__gpu_80gb = "always_on.gpu-80gb"
    # always_on__gpu_24gb__docker_cached_facilitator = "always_on.gpu-24gb.docker_cached-facilitator"


@dataclass
class ExecutorClassSpec:
    # mostly for user consumption, intended usage, ...
    description: str | None = None
    has_gpu: bool | None = None
    cpu_cores: int | None = None
    ram_gb: int | None = None
    storage_gb: int | None = None
    gpu_vram_gb: int | None = None
    spin_up_time: int | None = None  # seconds
    docker_cached_images: list[str] | None = None
    # requirements which can't be easily standardized
    additional_requirements: str | None = None


EXECUTOR_CLASS = {
    ExecutorClass.spin_up_4min__gpu_24gb: ExecutorClassSpec(
        description="cost effective (started on demand), generic GPU machine",
        has_gpu=True,
        gpu_vram_gb=24,
        spin_up_time=int(timedelta(minutes=4).total_seconds()),
    ),
    ExecutorClass.always_on__gpu_24gb: ExecutorClassSpec(
        description="always on, generic GPU machine",
        has_gpu=True,
        gpu_vram_gb=24,
        spin_up_time=0,
    ),
    ExecutorClass.always_on__llm__a6000: ExecutorClassSpec(
        description="always on, NVIDIA RTX A6000 GPU machine for LLM prompts solving",
        has_gpu=True,
        gpu_vram_gb=48,
        spin_up_time=int(timedelta(minutes=1).total_seconds()),
    ),
    # ExecutorClass.always_on__cpu_16c__ram_64gb: ExecutorClassSpec(
    #     cpu_cores=16,
    #     ram_gb=64,
    #     spin_up_time=0,
    # ),
    # ExecutorClass.always_on__gpu_80gb: ExecutorClassSpec(
    #     has_gpu=True,
    #     gpu_vram_gb=80,
    #     spin_up_time=0,
    # ),
    # ExecutorClass.always_on__gpu_24gb__docker_cached_facilitator: ExecutorClassSpec(
    #     has_gpu=True,
    #     gpu_vram_gb=24,
    #     spin_up_time=0,
    #     docker_cached_images=[
    #         "backenddevelopersltd/compute-horde-job-huggingface",
    #         "backenddevelopersltd/compute-horde-job-ffmpeg:vidstab-v0",
    #     ],
    # ),
}


# we split 144min 2 tempos window to 24 validators - this is total time after reservation,
# validator may wait spin_up time of executor class to synchronize running synthetic batch
MAX_EXECUTOR_TIMEOUT = timedelta(minutes=6).total_seconds()

DEFAULT_EXECUTOR_CLASS = ExecutorClass.spin_up_4min__gpu_24gb
DEFAULT_EXECUTOR_TIMEOUT = EXECUTOR_CLASS[DEFAULT_EXECUTOR_CLASS].spin_up_time
