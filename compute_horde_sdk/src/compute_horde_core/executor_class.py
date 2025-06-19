from compute_horde_core.compatibility import StrEnum


class ExecutorClass(StrEnum):
    spin_up_4min__gpu_24gb = "spin_up-4min.gpu-24gb"
    always_on__gpu_24gb = "always_on.gpu-24gb"
    always_on__llm__a6000 = "always_on.llm.a6000"
