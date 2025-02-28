from enum import StrEnum


class ExecutorClass(StrEnum):
    spin_up_4min__gpu_24gb = "spin_up-4min.gpu-24gb"
    always_on__gpu_24gb = "always_on.gpu-24gb"
    always_on__llm__a6000 = "always_on.llm.a6000"
    # always_on__cpu_16c__ram_64gb = "always_on.cpu-16c.ram-64gb"
    # always_on__gpu_80gb = "always_on.gpu-80gb"
    # always_on__gpu_24gb__docker_cached_facilitator = "always_on.gpu-24gb.docker_cached-facilitator"
