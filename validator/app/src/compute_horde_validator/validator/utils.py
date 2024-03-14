import time
from datetime import timedelta


class Timer:
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.elapsed = timedelta(seconds=time.time() - self.start)
