# pdm add orjson

import argparse
import asyncio
import logging
from dataclasses import dataclass

import orjson
import uvloop
import websockets
from compute_horde.executor_class import ExecutorClass
from compute_horde.mv_protocol.miner_requests import (
    ExecutorClassManifest,
    ExecutorManifest,
    V0AcceptJobRequest,
    V0ExecutorManifestRequest,
    V0ExecutorReadyRequest,
    V0JobFinishedRequest,
)

logger = logging.getLogger(__name__)


@dataclass
class JobState:
    job_uuid: str
    state: str = "?"
    executor_class: str = "?"


class MinerConn:
    def __init__(self):
        self.state = "."
        self.job_state: dict[int, JobState] = {}
        self.job_state_index: dict[str, int] = {}

    def get_job_state(self, job_uuid: str) -> JobState:
        job_index = self.job_state_index.get(job_uuid)
        if job_index is None:
            job_index = len(self.job_state)
            self.job_state_index[job_uuid] = job_index
            self.job_state[job_index] = JobState(job_uuid)
        return self.job_state[job_index]

    async def ws_handler(self, ws: websockets.WebSocketServerProtocol) -> None:
        self.state = "C"
        try:
            async for msg in ws:
                obj = orjson.loads(msg)
                print("<<<", obj)

                responses = []
                msg_type = obj["message_type"]
                match msg_type:
                    case "V0AuthenticateRequest":
                        self.state = "A"
                        responses.append(
                            V0ExecutorManifestRequest(
                                manifest=ExecutorManifest(
                                    executor_classes=[
                                        ExecutorClassManifest(
                                            executor_class=ExecutorClass.always_on__gpu_24gb,
                                            count=1,
                                        ),
                                        ExecutorClassManifest(
                                            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
                                            count=1,
                                        ),
                                    ]
                                )
                            )
                        )

                    case "V0InitialJobRequest":
                        job_uuid = obj["job_uuid"]
                        job_state = self.get_job_state(job_uuid)
                        job_state.state = "I"
                        job_state.executor_class = obj["executor_class"]
                        responses.append(V0AcceptJobRequest(job_uuid=job_uuid))
                        responses.append(V0ExecutorReadyRequest(job_uuid=job_uuid))

                        # V0DeclineJobRequest
                        # V0ExecutorFailedRequest

                    case "V0JobRequest":
                        job_uuid = obj["job_uuid"]
                        job_state = self.get_job_state(job_uuid)
                        job_state.state = "R"
                        responses.append(
                            V0JobFinishedRequest(
                                job_uuid=job_uuid,
                                docker_process_stdout="",
                                docker_process_stderr="",
                            )
                        )

                        # V0JobFailedRequest
                        # V0JobFinishedRequest

                    case "V0JobStartedReceiptRequest":
                        job_uuid = obj["payload"]["job_uuid"]
                        job_state = self.get_job_state(job_uuid)
                        job_state.state = "S"

                    case "V0JobFinishedReceiptRequest":
                        job_uuid = obj["payload"]["job_uuid"]
                        job_state = self.get_job_state(job_uuid)
                        job_state.state = "F"

                    case _:
                        continue

                for response in responses:
                    print(">>>", response)
                    await ws.send(response.model_dump_json())

                await asyncio.sleep(2)

            self.state = "-"

        except Exception as exc:
            logger.exception(exc)
            self.state = "X"


class Miner:
    def __init__(self, uid: int, port: int):
        self.uid = uid
        self.port = port
        self.conn: dict[int, MinerConn | None] = {}

    async def run(self) -> None:
        async with websockets.serve(
            self.__ws_handler, host="127.0.0.1", port=self.port, compression=None
        ):
            # sleep forever
            await asyncio.sleep(1e9)

    async def __ws_handler(self, ws: websockets.WebSocketServerProtocol, _path: str) -> None:
        conn_index = len(self.conn)
        try:
            conn = MinerConn()
            self.conn[conn_index] = conn
            await conn.ws_handler(ws)
        finally:
            self.conn[conn_index] = None


async def monitor(miners: list[Miner]) -> None:
    while True:
        lines = []
        lines.append("-" * 50)
        for miner in miners:
            for conn_index, conn in miner.conn.items():
                if conn is not None:
                    line = f"{miner.uid:3} {conn_index:3} {conn.state} "
                    parts = []
                    for _job_index, job_state in sorted(conn.job_state.items()):
                        part = f"{job_state.executor_class[0]}{job_state.state}"
                        parts.append(part)
                    line += " ".join(parts)
                else:
                    line = f"{miner.uid:3} {conn_index:3} -"
                lines.append(line)
        print("\n".join(lines))
        await asyncio.sleep(1)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # use argparse to read two command line arguments, port and count, both integers
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=int, default=8000)
    parser.add_argument("-c", "--count", type=int, default=1)
    args = parser.parse_args()

    logging.info("%s miners on ports %s -> %s", args.count, args.port, args.port + args.count - 1)

    async with asyncio.TaskGroup() as tg:
        tasks = []
        miners = []
        for uid in range(args.count):
            miner = Miner(uid, args.port + uid)
            miners.append(miner)
            task = tg.create_task(miner.run(), name=f"serve-miner-{uid}")
            tasks.append(task)
        task = tg.create_task(monitor(miners), name="monitor")
        tasks.append(task)

    logging.info("EXIT")


if __name__ == "__main__":
    uvloop.run(main())
