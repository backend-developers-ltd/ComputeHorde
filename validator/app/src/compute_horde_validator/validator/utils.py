import base64
import datetime as dt
import io
import zipfile


def single_file_zip(filename: str, contents: str) -> str:
    in_memory_output = io.BytesIO()
    zipf = zipfile.ZipFile(in_memory_output, 'w')
    zipf.writestr(filename, contents)
    zipf.close()
    in_memory_output.seek(0)
    zip_contents = in_memory_output.read()
    return base64.b64encode(zip_contents).decode()

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
