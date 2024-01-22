import importlib

from django.conf import settings

from compute_horde_miner.miner.executor_manager.base import BaseExecutorManager

module_path, class_name = settings.EXECUTOR_MANAGER_CLASS_PATH.split(":", 1)
target_module = importlib.import_module(module_path)
klass = getattr(target_module, class_name)

executor_manager: BaseExecutorManager = klass()
