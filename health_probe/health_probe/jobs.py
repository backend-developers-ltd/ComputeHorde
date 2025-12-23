from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec
from compute_horde_sdk.v1 import ExecutorClass, InlineInputVolume, HuggingfaceInputVolume

hello_world = ComputeHordeJobSpec(
    executor_class=ExecutorClass.always_on__cpu__8c__16gb,
    job_namespace="SN123.0",
    docker_image="alpine",
    args=["sh", "-c", "echo 'Hello, World!' > /artifacts/stuff"],
    artifacts_dir="/artifacts",
    download_time_limit_sec=60,
    execution_time_limit_sec=30,
    upload_time_limit_sec=10,
)

huggingface = ComputeHordeJobSpec(
    executor_class=ExecutorClass.always_on__cpu__8c__16gb,
    job_namespace="SN56.0",
    docker_image="acreef/horde:volume_list",
    download_time_limit_sec=300,
    execution_time_limit_sec=30,
    streaming_start_time_limit_sec=60,
    upload_time_limit_sec=60,
    artifacts_dir="/artifacts",
    input_volumes={
        "/volume/inline": InlineInputVolume.from_file_contents(
            filename="inline_data.txt",
            contents=b"\nINLINE DATA\n",
        ),
        "/volume/model_a": HuggingfaceInputVolume(
            repo_id="bghira/terminus-xl-velocity-v2",
            repo_type="model",
        ),
        "/volume/model_b": HuggingfaceInputVolume(
            repo_id="nestija/4041ae09-09f5-4dea-8a8e-a90cbfe7838d",
            repo_type="model",
        ),
    },
)
