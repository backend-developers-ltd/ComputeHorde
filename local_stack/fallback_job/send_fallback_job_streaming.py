import asyncio
import httpx
import logging
from compute_horde_sdk.v1.fallback import FallbackClient, FallbackJobSpec
from compute_horde_sdk.v1 import ComputeHordeJobSpec, ExecutorClass, InlineInputVolume


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


STREAMING_SERVER_CODE = """from fastapi import FastAPI, BackgroundTasks
import os, signal, threading

app = FastAPI()

@app.get("/message")
def root(): 
    return {"message": "Some message."}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.post("/terminate")
def terminate(background_tasks: BackgroundTasks):
    def shutdown(): 
        os._exit(0)
    background_tasks.add_task(shutdown)
    return {"message": "Server is shutting down."}
"""

async def main():
    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        job_namespace="SN123.0",
        docker_image="python:3.11-slim",
        args=[
            "pip install --no-cache-dir fastapi uvicorn && "
            "cd /volume && uvicorn app:app --host 127.0.0.1 --port 8000"
        ],
        output_volumes=None,
        artifacts_dir="/output",
        download_time_limit_sec=5,
        execution_time_limit_sec=10,
        upload_time_limit_sec=5,
        streaming=True,
        streaming_start_time_limit_sec=10,
        input_volumes={
            "/volume/": InlineInputVolume.from_file_contents(
                "app.py",
                STREAMING_SERVER_CODE.encode('utf-8')
            )
        }
    )

    fallback_job_spec = FallbackJobSpec.from_job_spec(
        compute_horde_job_spec, 
        work_dir="/"
    )

    with FallbackClient(cloud="runpod", idle_minutes=1) as fallback_client:
        job = await fallback_client.create_job(fallback_job_spec)
        await job.wait_for_streaming(timeout=120)

        url = f"http://{job.streaming_server_address}:{job.streaming_server_port}"

        with httpx.Client(timeout=5.0) as client:
            message_url = f"{url}/message"
            logger.info(f"[Fallback] Testing /message endpoint at {message_url}")
            resp = client.get(message_url)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("message") == "Some message.":
                    logger.info("[Fallback] Successfully verified /message endpoint")
                else:
                    raise RuntimeError(f"[Fallback] Unexpected message content: {data}")
            else:
                raise RuntimeError(f"[Fallback] Failed to get /message: {resp.status_code}")

            # Terminate the server
            terminate_url = f"{url}/terminate"
            logger.info(f"[Fallback] Attempting to terminate server at {terminate_url}")
            resp = client.post(terminate_url)
            if resp.status_code == 200:
                logger.info("[Fallback] Server terminated successfully")
            else:
                raise RuntimeError(f"[Fallback] Failed to terminate server: {resp.status_code}")

        await job.wait(timeout=120)
    logger.info("Success!")

if __name__ == "__main__":
    asyncio.run(main())
