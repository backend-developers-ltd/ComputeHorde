import sys

# Backward compatibility for StrEnum with python 3.10
if sys.version_info >= (3, 11):  # noqa: UP036
    from enum import StrEnum
else:
    from enum import Enum

    class StrEnum(str, Enum):
        def __str__(self) -> str:
            return str(self.value)


__all__ = ["StrEnum"]
