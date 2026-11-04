import subprocess
import sys
from types import ModuleType


def test_apiver_exports(apiver_module):
    assert {
        name
        for name in dir(apiver_module)
        if not name.startswith("_") and not isinstance(getattr(apiver_module, name), ModuleType)
    } == {
        "ComputeHordeClient",
        "ComputeHordeError",
        "ComputeHordeHordeFailure",
        "ComputeHordeNotFoundError",
        "ComputeHordeJobTimeoutError",
        "ComputeHordeJobSpec",
        "ComputeHordeJob",
        "ComputeHordeJobFailure",
        "ComputeHordeJobRejection",
        "ComputeHordeJobStatus",
        "ComputeHordeJobStatusEntry",
        "ComputeHordeJobResult",
        "ExecutorClass",
        "HTTPInputVolume",
        "HTTPOutputVolume",
        "HuggingfaceInputVolume",
        "InlineInputVolume",
        "InputVolume",
        "OutputVolume",
    }


def test_remote_import_without_docker():
    result = subprocess.run(
        [sys.executable, "-c", "import sys; sys.modules['docker'] = None; import compute_horde_sdk.v1"],
        capture_output=True,
        text=True,
    )
    assert (result.returncode, result.stdout, result.stderr) == (0, "", "")
