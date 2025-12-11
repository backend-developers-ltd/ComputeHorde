from compute_horde_core.executor_class import ExecutorClass

from compute_horde.executor_class import EXECUTOR_CLASS


def test_all_executor_classes_are_in_mapping():
    missing_classes = []
    for executor_class in ExecutorClass:
        if executor_class not in EXECUTOR_CLASS:
            missing_classes.append(executor_class)

    assert missing_classes == [], (
        f"The following ExecutorClass values are missing from EXECUTOR_CLASS: "
        f"{missing_classes}. Please add them to the mapping."
    )
