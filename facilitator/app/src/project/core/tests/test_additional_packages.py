from importlib import import_module
from pathlib import Path

from django.conf import settings

# import tests from additional packages into local namespace, so that pytest can pick them up;
# it is expected that additional package has a folder `tests` with `test_xxx.py` files,
# with `def test_yyy` functions
for package in settings.ADDITIONAL_APPS:
    module = import_module(f"{package}.tests")
    for file in Path(module.__path__[0]).iterdir():
        name = file.stem
        if name.startswith("test_"):
            test_module = import_module(f"{package}.tests.{name}")
            for name in dir(test_module):
                if name.startswith("test_") and callable(method := getattr(test_module, name)):
                    locals()[name] = method
