from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum


class ExecutorClass(StrEnum):
    spin_up_4min__gpu_24gb = "spin_up-4min.gpu-24gb"
    always_on__gpu_24gb = "always_on.gpu-24gb"
    always_on__llm__a6000 = "always_on.llm.a6000"


@dataclass
class ExecutorClassSpec:
    # mostly for user consumption, intended usage, ...
    description: str
    spin_up_time: int
    has_gpu: bool
    cpu_cores: int | None = None
    ram_gb: int | None = None
    storage_gb: int | None = None
    gpu_vram_gb: int | None = None
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
        spin_up_time=int(timedelta(minutes=4).total_seconds()),
    ),
}


MAX_EXECUTOR_TIMEOUT = timedelta(minutes=20).total_seconds()

DEFAULT_EXECUTOR_CLASS = ExecutorClass.spin_up_4min__gpu_24gb
DEFAULT_LLM_EXECUTOR_CLASS = ExecutorClass.always_on__llm__a6000
DEFAULT_EXECUTOR_TIMEOUT = EXECUTOR_CLASS[DEFAULT_EXECUTOR_CLASS].spin_up_time
