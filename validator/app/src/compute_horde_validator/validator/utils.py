import datetime as dt


class Timer:
    def __init__(self, timeout=None):
        self.start_time = dt.datetime.now()
        self.timeout = timeout

    def passed_time(self):
        return (dt.datetime.now() - self.start_time).total_seconds()

    def time_left(self):
        if self.timeout is None:
            raise ValueError('timeout was not specified')
        return self.timeout - self.passed_time()
