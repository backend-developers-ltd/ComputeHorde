def test_apiver_exports(apiver_module):
    assert {name for name in dir(apiver_module) if not name.startswith("_")} == {
        "ComputeHordeClient",
        "ComputeHordeError",
        "ComputeHordeNotFoundError",
        "ComputeHordeJobTimeoutError",
        "ComputeHordeJob",
        "ComputeHordeJobStatus",
        "ExecutorClass",
        "HTTPInputVolume",
        "HTTPOutputVolume",
        "HuggingfaceInputVolume",
        "InlineInputVolume",
        "InputVolume",
        "OutputVolume",
    }
