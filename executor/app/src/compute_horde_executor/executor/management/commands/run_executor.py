import asyncio
import base64
import io
import logging
import pathlib
import shutil
import subprocess
import tempfile
import time
import zipfile

import pydantic
import websockets
from compute_horde.base_requests import ValidationError
from compute_horde.em_protocol.executor_requests import (
    BaseExecutorRequest,
    GenericError,
    V0FailedRequest,
    V0FailedToPrepare,
    V0FinishedRequest,
    V0ReadyRequest,
)
from compute_horde.em_protocol.miner_requests import (
    BaseMinerRequest,
    V0InitialJobRequest,
    V0JobRequest,
    VolumeType,
)
from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)

temp_dir = pathlib.Path(tempfile.mkdtemp())
volume_mount_dir = temp_dir / 'volume'


class MinerClient:
    def __init__(self, loop: asyncio.AbstractEventLoop, miner_address: str, token: str):
        self.loop = loop
        self.token = token
        self.miner_address = miner_address
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.job_uuid: str | None = None
        self.initial_msg = asyncio.Future()
        self.initial_msg_lock = asyncio.Lock()
        self.full_payload = asyncio.Future()
        self.full_payload_lock = asyncio.Lock()
        self.read_messages_task: asyncio.Task | None = None

    async def __aenter__(self):
        await self.await_connect()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.read_messages_task is not None and not self.read_messages_task.done():
            self.read_messages_task.cancel()

        if self.ws is not None and not self.ws.closed:
            await self.ws.close()

    async def _connect(self):
        return await websockets.connect(f'{self.miner_address}/v0/executor_interface/{self.token}')

    async def await_connect(self):
        while True:
            try:
                self.ws = await self._connect()
                self.read_messages_task = self.loop.create_task(self.read_messages())
                return
            except websockets.WebSocketException as ex:
                logger.error(f'Could not connect to miner: {str(ex)}')
            await asyncio.sleep(1)

    async def ensure_connected(self):
        if self.ws is None or self.ws.closed:
            if self.read_messages_task is not None and not self.read_messages_task.done():
                self.read_messages_task.cancel()
            await self.await_connect()

    async def read_messages(self):
        while True:
            try:
                msg = await self.ws.recv()
            except websockets.WebSocketException as ex:
                logger.error(f'Connection to miner lost: {str(ex)}')
                self.loop.create_task(self.await_connect())
                return

            try:
                msg = BaseMinerRequest.parse(msg)
            except ValidationError as ex:
                logger.error(f'Malformed message from miner: {str(ex)}')
                await self.ws.send(GenericError(details=f'Malformed message: {str(ex)}').model_dump_json())
                continue

            if isinstance(msg, V0InitialJobRequest):
                await self.handle_initial_job_request(msg)
            elif isinstance(msg, V0JobRequest):
                await self.handle_job_request(msg)
            elif isinstance(msg, GenericError):
                try:
                    raise RuntimeError(f'Received error message: {msg.model_dump_json()}')
                except Exception:
                    logger.exception('')
            else:
                try:
                    raise NotImplementedError(f'Received unsupported message: {msg.model_dump_json()}')
                except Exception:
                    logger.exception('')

    async def handle_initial_job_request(self, msg: V0InitialJobRequest):
        async with self.initial_msg_lock:
            if self.initial_msg.done():
                msg = f'Received duplicate initial job request: first {self.job_uuid=} and then {msg.job_uuid=}'
                logger.error(msg)
                await self.ws.send(GenericError(details=msg).model_dump_json())
                return
            self.job_uuid = msg.job_uuid
            logger.debug(f'Received initial job request: {msg.job_uuid=}')
            self.initial_msg.set_result(msg)

    async def handle_job_request(self, msg: V0JobRequest):
        async with self.full_payload_lock:
            if not self.initial_msg.done():
                msg = f'Received job request before an initial job request {msg.job_uuid=}'
                logger.error(msg)
                await self.ws.send(GenericError(details=msg).model_dump_json())
                return
            if self.full_payload.done():
                msg = (f'Received duplicate full job payload request: first '
                       f'{self.job_uuid=} and then {msg.job_uuid=}')
                logger.error(msg)
                await self.ws.send(GenericError(details=msg).model_dump_json())
                return
            logger.debug(f'Received full job payload request: {msg.job_uuid=}')
            self.full_payload.set_result(msg)

    async def send_model(self, model: BaseExecutorRequest):
        await self.ensure_connected()
        await self.ws.send(model.model_dump_json())

    async def send_ready(self):
        await self.send_model(V0ReadyRequest(job_uuid=self.job_uuid))

    async def send_finished(self, job_result: 'JobResult'):
        await self.send_model(V0FinishedRequest(
            job_uuid=self.job_uuid,
            docker_process_stdout=job_result.stdout,
            docker_process_stderr=job_result.stderr,
        ))

    async def send_failed(self, job_result: 'JobResult'):
        await self.send_model(V0FailedRequest(
            job_uuid=self.job_uuid,
            docker_process_exit_status=job_result.exit_status,
            timeout=job_result.timeout,
            docker_process_stdout=job_result.stdout,
            docker_process_stderr=job_result.stderr,
        ))

    async def send_generic_error(self, details: str):
        await self.send_model(GenericError(
            details=details,
        ))

    async def send_failed_to_prepare(self):
        await self.send_model(V0FailedToPrepare(
            job_uuid=self.job_uuid,
        ))


class JobResult(pydantic.BaseModel):
    success: bool
    exit_status: int | None
    timeout: bool
    stdout: str
    stderr: str


class JobError(Exception):
    def __init__(self, description: str):
        self.description = description


class JobRunner:
    def __init__(self, initial_job_request: V0InitialJobRequest):
        self.initial_job_request = initial_job_request

    async def prepare(self):

        process = await asyncio.create_subprocess_exec(
            'docker', 'pull', self.initial_job_request.base_docker_image_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            msg = (f'"docker pull {self.initial_job_request.base_docker_image_name}" '
                   f'(job_uuid={self.initial_job_request.job_uuid})'
                   f' failed with status={process.returncode}'
                   f' stdout="{stdout.decode()}"\nstderr="{stderr.decode()}')
            logger.error(msg)
            raise JobError(msg)

    async def run_job(self, job_request: V0JobRequest):
        self.unpack_volume(job_request)
        cmd = ['docker', 'run', '-v', f'{volume_mount_dir.as_posix()}/:/volume/', job_request.docker_image_name]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        t1 = time.time()
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(),
                                                    timeout=self.initial_job_request.timeout_seconds)

        except asyncio.TimeoutError:
            # If the process did not finish in time, kill it
            logger.error(f'Process didn\'t finish in time, killing it, job_uuid={self.initial_job_request.job_uuid}')
            process.kill()
            timeout = True
            exit_status = None
            stdout = (await process.stdout.read()).decode()
            stderr = (await process.stderr.read()).decode()
        else:
            stdout = stdout.decode()
            stderr = stderr.decode()
            exit_status = process.returncode
            timeout = False

        time_took = time.time() - t1
        success = exit_status == 0

        if success:
            logger.info(f'Job "{self.initial_job_request.job_uuid}" finished successfully in {time_took:0.2f} seconds')
        else:
            logger.error(f'"{" ".join(cmd)}" (job_uuid={self.initial_job_request.job_uuid})'
                         f' failed after {time_took:0.2f} seconds with status={process.returncode}'
                         f' \nstdout="{stdout}"\nstderr="{stderr}')

        return JobResult(
            success=success,
            exit_status=exit_status,
            timeout=timeout,
            stdout=stdout,
            stderr=stderr,
        )

    def unpack_volume(self, job_request: V0JobRequest):
        assert str(volume_mount_dir) not in {'~', '/'}
        if job_request.volume.volume_type == VolumeType.inline:
            for path in volume_mount_dir.glob("*"):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    shutil.rmtree(path)

            volume_mount_dir.mkdir(exist_ok=True)

            decoded_contents = base64.b64decode(job_request.volume.contents)
            bytes_io = io.BytesIO(decoded_contents)
            zip_file = zipfile.ZipFile(bytes_io)
            zip_file.extractall(volume_mount_dir.as_posix())

            subprocess.check_call(["chmod", "-R", "777", temp_dir.as_posix()])

        else:
            raise NotImplementedError(f'Unsupported volume_type: {job_request.volume.volume_type}')


class Command(BaseCommand):
    help = 'Run the executor, query the miner for job details, and run the job docker'

    MINER_CLIENT_CLASS = MinerClient
    JOB_RUNNER_CLASS = JobRunner

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.loop = asyncio.get_event_loop()
        self.miner_client = self.MINER_CLIENT_CLASS(self.loop, settings.MINER_ADDRESS, settings.EXECUTOR_TOKEN)

    def handle(self, *args, **options):
        self.loop.run_until_complete(self._executor_loop())

    async def _executor_loop(self):
        logger.debug(f'Connecting to miner: {settings.MINER_ADDRESS}')
        async with self.miner_client:
            logger.debug(f'Connected to miner: {settings.MINER_ADDRESS}')

            initial_message: V0InitialJobRequest = await self.miner_client.initial_msg
            try:
                job_runner = self.JOB_RUNNER_CLASS(initial_message)
                logger.debug(f'Preparing for job {initial_message.job_uuid}')
                try:
                    await job_runner.prepare()
                except JobError:
                    await self.miner_client.send_failed_to_prepare()
                    return

                logger.debug(f'Prepared for job {initial_message.job_uuid}')

                await self.miner_client.send_ready()
                logger.debug(f'Informed miner that I\'m ready for job {initial_message.job_uuid}')

                job_request = await self.miner_client.full_payload
                logger.debug(f'Running job {initial_message.job_uuid}')
                result = await job_runner.run_job(job_request)

                if result.success:
                    await self.miner_client.send_finished(result)
                else:
                    await self.miner_client.send_failed(result)
            except Exception:
                logger.error(f'Unhandled exception when working on job {initial_message.job_uuid}', exc_info=True)
                await self.miner_client.send_generic_error('Unexpected error')
