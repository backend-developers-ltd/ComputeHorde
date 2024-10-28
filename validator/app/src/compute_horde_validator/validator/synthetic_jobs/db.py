import logging
import time

from collections.abc import Iterable
from itertools import islice
from typing import TypeVar

from django.db.models import Model

logger = logging.getLogger(__name__)

# Create a TypeVar that's bound to models.Model
T = TypeVar("T", bound=Model)


def safe_bulk_create(
    model_class: type[T],
    objects_to_create: Iterable[T],
    batch_size: int = 1000,
    timeout: float | None = None,
    ignore_conflicts: bool = False,
):
    start = time.time()

    objs = (model for model in objects_to_create)

    result_objects: list[T] = []

    while True:
        batch = list(islice(objs, batch_size))
        if not batch:
            break
        result_objects.extend(
            model_class.objects.bulk_create(batch, batch_size, ignore_conflicts=ignore_conflicts)
        )

        if timeout and time.time() - start > timeout:
            logger.error("Bulk create operation timed out: model=%s", str(model_class))
            break
    return result_objects
