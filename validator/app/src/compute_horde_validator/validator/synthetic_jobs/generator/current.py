import importlib

from django.conf import settings

from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGeneratorFactory,
)

module_path, class_name = settings.SYNTHETIC_JOB_GENERATOR_FACTORY.split(":", 1)
target_module = importlib.import_module(module_path)
klass = getattr(target_module, class_name)

synthetic_job_generator_factory: BaseSyntheticJobGeneratorFactory = klass()
