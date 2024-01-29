import importlib

from django.conf import settings

from compute_horde_validator.validator.synthetic_jobs.generator.base import AbstractSyntheticJobGenerator

module_path, class_name = settings.SYNTHETIC_JOB_GENERATOR.split(":", 1)
target_module = importlib.import_module(module_path)
SyntheticJobGenerator: type[AbstractSyntheticJobGenerator] = getattr(target_module, class_name)
