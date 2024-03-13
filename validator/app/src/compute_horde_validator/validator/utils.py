import time
from collections.abc import Callable
from datetime import timedelta


class Timer:
    def __init__(self, name: str = "", output_fn: Callable | None = None):
        self.name = name
        self.output_fn = output_fn

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.elapsed = timedelta(seconds=self.end - self.start)
        if self.output_fn:
            self.output_fn(f"{self.name} took {self.interval:.2f} seconds")
