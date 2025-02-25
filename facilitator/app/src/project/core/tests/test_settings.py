from importlib import import_module

import pytest


def test__settings__celery_beat_schedule(settings):
    if not hasattr(settings, "CELERY_BEAT_SCHEDULE"):
        pytest.skip("CELERY_BEAT_SCHEDULE is not defined")

    paths = {task["task"] for task in settings.CELERY_BEAT_SCHEDULE.values()}
    for path in paths:
        module_path, task_name = path.rsplit(".", maxsplit=1)
        try:
            module = import_module(module_path)
        except ImportError:
            pytest.fail(f"The module '{module_path}' does not exist")

        if not hasattr(module, task_name):
            pytest.fail(f"The task '{task_name}' does not exist in {module_path}")
