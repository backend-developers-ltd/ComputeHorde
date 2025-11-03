from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.dynamic_config import (
    executor_class_value_map_parser,
)


def test__executor_class_value_map_parser():
    assert executor_class_value_map_parser("always_on.llm.a6000=1", value_parser=float) == {
        ExecutorClass.always_on__llm__a6000: 1.0
    }
    assert executor_class_value_map_parser(
        "always_on.llm.a6000=this,spin_up-4min.gpu-24gb=that"
    ) == {ExecutorClass.spin_up_4min__gpu_24gb: "that", ExecutorClass.always_on__llm__a6000: "this"}
    assert executor_class_value_map_parser("", value_parser=float) == {}
