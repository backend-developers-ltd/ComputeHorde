import os
import json
import uuid
import random
import asyncio
import traceback
from datetime import datetime, UTC

import uvloop
import bittensor_wallet
from pydantic import TypeAdapter

from compute_horde_sdk.v1 import (
    ComputeHordeClient,
    ComputeHordeJob,
    ComputeHordeJobSpec,
    ComputeHordeJobResult,
    ComputeHordeJobStatusEntry,
    ExecutorClass,
    ComputeHordeJobStatus,
    InputVolume,
    InlineInputVolume,
    # HuggingfaceInputVolume,
)

DATA_DIR = os.path.expanduser("~/.local/share/computehorde/stress")

SIGNING_WALLET_NAME = "_stress_sn12"
TARGET_VALIDATOR_HOTKEY = "5HBVrFGy6oYhhh71m9fFGYD7zbKyAeHnWN8i8s9fJTBMCtEE"

EXECUTOR_CLASS = ExecutorClass.always_on__test
JOB_DOCKER_IMAGE = "acreef/horde:volume_list"
JOB_NAMESPACE = "SN12.0"

JOB_COUNT = 25
JOB_TIMEOUT = 600

JOB_BOARD_STATE = {
    ComputeHordeJobStatus.PENDING: "P",
    ComputeHordeJobStatus.SENT: "S",
    ComputeHordeJobStatus.RECEIVED: "R",
    ComputeHordeJobStatus.ACCEPTED: "A",
    ComputeHordeJobStatus.REJECTED: "X",
    ComputeHordeJobStatus.STREAMING_READY: "T",
    ComputeHordeJobStatus.EXECUTOR_READY: "E",
    ComputeHordeJobStatus.VOLUMES_READY: "V",
    ComputeHordeJobStatus.EXECUTION_DONE: "D",
    ComputeHordeJobStatus.COMPLETED: "C",
    ComputeHordeJobStatus.FAILED: "F",
    ComputeHordeJobStatus.HORDE_FAILED: "H",
}

ComputeHordeJobSpecAdapter = TypeAdapter(ComputeHordeJobSpec)
ComputeHordeJobResultAdapter = TypeAdapter(ComputeHordeJobResult)
ComputeHordeJobStatusEntryAdapter = TypeAdapter(ComputeHordeJobStatusEntry)


class StatusLogger:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        time_str = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
        status_filename = f"stress-{time_str}.jsonl"
        status_path = os.path.join(DATA_DIR, status_filename)
        self._file = open(status_path, "wb", buffering=0)

    def write(self, job_id: str, kind: str, payload: dict) -> None:
        data = {
            "id": job_id,
            "ts": datetime.now(tz=UTC).isoformat(),
            "kind": kind,
            "payload": payload,
        }
        data_raw = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        self._file.write(data_raw.encode("utf-8") + b"\n")

    def write_exc(self, job_id: str, exc: Exception) -> None:
        payload = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        self.write(job_id, "exception", payload)

    def write_job_spec(self, job_id: str, job_spec: ComputeHordeJobSpec) -> None:
        payload = ComputeHordeJobSpecAdapter.dump_python(job_spec)
        self.write(job_id, "job_spec", payload)

    def write_job(self, job_id: str, job: ComputeHordeJob) -> None:
        if job.result is not None:
            job_result = json.loads(ComputeHordeJobResultAdapter.dump_json(job.result))
        else:
            job_result = None

        if job.status_history is not None:
            job_status_history = [
                json.loads(ComputeHordeJobStatusEntryAdapter.dump_json(entry))
                for entry in job.status_history
            ]
        else:
            job_status_history = None

        payload = {
            "uuid": job.uuid,
            "status": job.status.value,
            "result": job_result,
            "status_history": job_status_history,
        }
        self.write(job_id, "job", payload)


async def run_job(
    compute_horde_client: ComputeHordeClient,
    status_logger: StatusLogger,
    job_board: dict[int, str],
    job_index: int,
) -> None:
    job_id = str(uuid.uuid4())

    status_logger.write(job_id, "enter", {})
    job_board[job_index] = "."
    try:
        async with asyncio.timeout(JOB_TIMEOUT):
            input_volumes: dict[str, InputVolume] = {
                "/volume/inline": InlineInputVolume.from_file_contents(
                    filename="inline_data.txt",
                    contents=b"\nINLINE DATA\n",
                ),
                # "/volume/model_a": HuggingfaceInputVolume(
                #     repo_id="bghira/terminus-xl-velocity-v2",
                #     repo_type="model",
                #     # revision="main",
                #     # allow_patterns=["model.safetensors"],
                # ),
                # "/volume/model_b": HuggingfaceInputVolume(
                #     repo_id="nestija/4041ae09-09f5-4dea-8a8e-a90cbfe7838d",
                #     repo_type="model",
                #     # revision="main",
                #     # allow_patterns="last.safetensors",
                # ),
            }

            sleep = random.randint(1, 60)
            job_spec = ComputeHordeJobSpec(
                executor_class=EXECUTOR_CLASS,
                job_namespace=JOB_NAMESPACE,
                docker_image=JOB_DOCKER_IMAGE,
                args=["--sleep", str(sleep)],
                download_time_limit_sec=10,
                execution_time_limit_sec=90,
                streaming_start_time_limit_sec=1,
                upload_time_limit_sec=5,
                artifacts_dir="/artifacts",
                input_volumes=input_volumes,
            )
            status_logger.write_job_spec(job_id, job_spec)
            job_board[job_index] = "J"

            job = await compute_horde_client.create_job(job_spec)

            while True:
                status_logger.write_job(job_id, job)
                job_board[job_index] = JOB_BOARD_STATE.get(job.status, "?")

                if job.status in (
                    ComputeHordeJobStatus.COMPLETED,
                    ComputeHordeJobStatus.REJECTED,
                    ComputeHordeJobStatus.FAILED,
                    ComputeHordeJobStatus.HORDE_FAILED,
                ):
                    break

                await asyncio.sleep(1)
                job = await compute_horde_client.get_job(job.uuid)

            if job.status == ComputeHordeJobStatus.COMPLETED:
                assert job.result is not None
                print("*" * 80)
                print(job.result.stdout.strip())
                print("*" * 80)
                for name in job.result.artifacts.keys():
                    print(f"ARTIFACT: {name}")
            elif job.result:
                print()
                print(job.result)
                print()
                if job.status_history is not None:
                    for status_entry in job.status_history:
                        print(f"{status_entry.created_at} {status_entry.status}")
                        if status_entry.metadata is not None:
                            if status_entry.metadata.job_rejection_details:
                                print(
                                    f"  {status_entry.metadata.job_rejection_details}"
                                )
                            if status_entry.metadata.job_failure_details:
                                print(f"  {status_entry.metadata.job_failure_details}")
                            if status_entry.metadata.horde_failure_details:
                                detail = status_entry.metadata.horde_failure_details
                                print(
                                    f"  {detail.reported_by} {detail.reason} {detail.message}"
                                )
                                if detail.context:
                                    for key, value in detail.context.items():
                                        print(f"    {key}: {value}")

    except asyncio.CancelledError:
        status_logger.write(job_id, "cancelled", {})

    except Exception as exc:
        print(f"Job {job_id} failed with exception: {exc}")
        status_logger.write_exc(job_id, exc)

    finally:
        status_logger.write(job_id, "leave", {})


async def main() -> None:
    wallet = bittensor_wallet.Wallet(name=SIGNING_WALLET_NAME, hotkey="default")
    compute_horde_client = ComputeHordeClient(
        hotkey=wallet.hotkey,
        compute_horde_validator_hotkey=TARGET_VALIDATOR_HOTKEY,
    )

    status_logger = StatusLogger()
    job_board: dict[int, str] = {}

    for job_index in range(JOB_COUNT):
        asyncio.create_task(
            run_job(compute_horde_client, status_logger, job_board, job_index)
        )

    while True:
        time_str = datetime.now(tz=UTC).strftime("%H:%M:%S")
        all_state = "".join(job_state for _, job_state in sorted(job_board.items()))
        print(f"{time_str}   {all_state}")
        await asyncio.sleep(1)


if __name__ == "__main__":
    uvloop.run(main())
