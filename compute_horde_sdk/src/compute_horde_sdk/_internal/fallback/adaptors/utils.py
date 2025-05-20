import importlib.machinery
import importlib.util
import sys
from typing import Any


def lazy_import_adaptor(adaptor: str) -> Any:
    """
    Lazily import a specific adaptor module from this package.

    :param adaptor: The adaptor module.
    :return: The imported module.
    """
    module_name = importlib.util.resolve_name(f".{adaptor}", __package__)
    if module_name in sys.modules:
        module = sys.modules[module_name]
    else:
        spec = importlib.util.find_spec(module_name)
        if not spec or not spec.loader:
            raise ModuleNotFoundError(f"Module '{module_name}' could not be found.")

        loader = importlib.util.LazyLoader(spec.loader)
        spec.loader = loader
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        loader.exec_module(module)

    return module
