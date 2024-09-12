import importlib

from django.conf import settings

from .base import BasePromptJobGenerator

module_path, class_name = settings.PROMPT_JOB_GENERATOR.split(":", 1)
target_module = importlib.import_module(module_path)
klass = getattr(target_module, class_name)

prompt_job_generator: BasePromptJobGenerator = klass
