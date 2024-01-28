import importlib

from django.conf import settings

from compute_horde_validator.validator.synthetic_jobs.generator.base import AbstractChallengeGenerator

module_path, class_name = settings.CHALLENGE_GENERATOR.split(":", 1)
target_module = importlib.import_module(module_path)
ChallengeGenerator: type[AbstractChallengeGenerator] = getattr(target_module, class_name)
