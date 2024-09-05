import base64
import io
import zipfile
from functools import cache

MACHINE_SPEC_GROUP_NAME = "machine_spec_sending"


def single_file_zip(filename: str, contents: str) -> str:
    in_memory_output = io.BytesIO()
    zipf = zipfile.ZipFile(in_memory_output, "w")
    zipf.writestr(filename, contents)
    zipf.close()
    in_memory_output.seek(0)
    zip_contents = in_memory_output.read()
    return base64.b64encode(zip_contents).decode()


@cache
def get_dummy_inline_zip_volume() -> str:
    in_memory_output = io.BytesIO()
    with zipfile.ZipFile(in_memory_output, "w"):
        pass
    in_memory_output.seek(0)
    zip_contents = in_memory_output.read()
    base64_zip_contents = base64.b64encode(zip_contents)
    return base64_zip_contents.decode()
