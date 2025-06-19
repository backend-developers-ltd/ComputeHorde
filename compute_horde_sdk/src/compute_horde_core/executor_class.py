try:
    from enum import StrEnum
except ImportError:
    # Backward compatible with python 3.10
    from enum import Enum

    class StrEnum(str, Enum):
        def __str__(self):
            return str(self.value)

class ExecutorClass(StrEnum):
    spin_up_4min__gpu_24gb = "spin_up-4min.gpu-24gb"
    always_on__gpu_24gb = "always_on.gpu-24gb"
    always_on__llm__a6000 = "always_on.llm.a6000"
