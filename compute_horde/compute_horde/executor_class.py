from dataclasses import dataclass
from datetime import timedelta


@dataclass
class ExecutorClass:
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
    "spin_up-4min.gpu-24gb": ExecutorClass(
        description="cost effective (started on demand), generic GPU machine",
        has_gpu=True,
        gpu_vram_gb=24,
        spin_up_time=int(timedelta(minutes=4).total_seconds()),
    ),
    # "always_on.cpu-16c.ram-64gb": ExecutorClass(
    #     cpu_cores=16,
    #     ram_gb=64,
    #     spin_up_time=0,
    # ),
    # "always_on.gpu-24gb": ExecutorClass(
    #     has_gpu=True,
    #     gpu_vram_gb=24,
    #     spin_up_time=0,
    # ),
    # "always_on.gpu-80gb": ExecutorClass(
    #     has_gpu=True,
    #     gpu_vram_gb=80,
    #     spin_up_time=0,
    # ),
    # "always_on.gpu-24gb.docker_cached-facilitator": ExecutorClass(
    #     has_gpu=True,
    #     gpu_vram_gb=24,
    #     spin_up_time=0,
    #     docker_cached_images=[
    #         "backenddevelopersltd/compute-horde-job-huggingface",
    #         "backenddevelopersltd/compute-horde-job-ffmpeg:vidstab-v0",
    #     ],
    # ),
}


# this leaves around 1 min for synthetic job to complete
MAX_EXECUTOR_TIMEOUT = timedelta(minutes=4).total_seconds()

DEFAULT_EXECUTOR_CLASS = "spin_up-4min.gpu-24gb"
DEFAULT_EXECUTOR_TIMEOUT = EXECUTOR_CLASS[DEFAULT_EXECUTOR_CLASS].spin_up_time
