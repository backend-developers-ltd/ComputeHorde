import csv
import logging
import re
import shutil
import subprocess
import typing
from contextlib import asynccontextmanager

import aiodocker
from compute_horde.utils import MachineSpecs

logger = logging.getLogger(__name__)


def run_cmd(cmd):
    proc = subprocess.run(cmd, shell=True, capture_output=True, check=False, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"run_cmd error {cmd=!r} {proc.returncode=} {proc.stdout=!r} {proc.stderr=!r}"
        )
    return proc.stdout


async def run_nvidia_smi():
    async with aiodocker.Docker() as client:
        container = await client.containers.create(
            {
                "Image": "ubuntu",
                "Cmd": [
                    "nvidia-smi",
                    "--query-gpu=name,driver_version,name,memory.total,compute_cap,power.limit,clocks.gr,clocks.mem,uuid,serial",
                    "--format=csv",
                ],
                "HostConfig": {
                    "Runtime": "nvidia",
                    "DeviceRequests": [
                        {
                            "Driver": "nvidia",
                            "Count": -1,
                            "Capabilities": [["gpu"]],
                        }
                    ],
                },
            }
        )  # type: aiodocker.containers.DockerContainer
        await container.start()
        result = await container.wait()
        stdout, stderr = await get_docker_container_outputs(container)
        await container.delete(force=True)

        if result["StatusCode"] != 0:
            raise RuntimeError(
                f"run_nvidia_smi error ({result['StatusCode']}) {stdout=!r} {stderr=!r}"
            )

        return stdout


@typing.no_type_check
async def get_machine_specs() -> MachineSpecs:
    data = {}

    data["gpu"] = {"count": 0, "details": []}
    try:
        nvidia_cmd = await run_nvidia_smi()
        csv_data = csv.reader(nvidia_cmd.splitlines())
        header = [x.strip() for x in next(csv_data)]
        for row in csv_data:
            row = [x.strip() for x in row]
            gpu_data = dict(zip(header, row))
            data["gpu"]["details"].append(
                {
                    "name": gpu_data["name"],
                    "driver": gpu_data["driver_version"],
                    "capacity": gpu_data["memory.total [MiB]"].split(" ")[0],
                    "cuda": gpu_data["compute_cap"],
                    "power_limit": gpu_data["power.limit [W]"].split(" ")[0],
                    "graphics_speed": gpu_data["clocks.current.graphics [MHz]"].split(" ")[0],
                    "memory_speed": gpu_data["clocks.current.memory [MHz]"].split(" ")[0],
                    "uuid": gpu_data["uuid"].split(" ")[0],
                    "serial": gpu_data["serial"].split(" ")[0],
                }
            )
        data["gpu"]["count"] = len(data["gpu"]["details"])
    except Exception as exc:
        # print(f'Error processing scraped gpu specs: {exc}', flush=True)
        data["gpu_scrape_error"] = repr(exc)

    data["cpu"] = {"count": 0, "model": "", "clocks": []}
    try:
        lscpu_output = run_cmd("lscpu")
        data["cpu"]["model"] = re.search(r"Model name:\s*(.*)$", lscpu_output, re.M).group(1)
        data["cpu"]["count"] = int(re.search(r"CPU\(s\):\s*(.*)", lscpu_output).group(1))

        cpu_data = run_cmd('lscpu --parse=MHZ | grep -Po "^[0-9,.]*$"').splitlines()
        data["cpu"]["clocks"] = [float(x) for x in cpu_data]
    except Exception as exc:
        # print(f'Error getting cpu specs: {exc}', flush=True)
        data["cpu_scrape_error"] = repr(exc)

    data["ram"] = {}
    try:
        with open("/proc/meminfo") as f:
            meminfo = f.read()

        for name, key in [
            ("MemAvailable", "available"),
            ("MemFree", "free"),
            ("MemTotal", "total"),
        ]:
            data["ram"][key] = int(re.search(rf"^{name}:\s*(\d+)\s+kB$", meminfo, re.M).group(1))
        data["ram"]["used"] = data["ram"]["total"] - data["ram"]["free"]
    except Exception as exc:
        # print(f"Error reading /proc/meminfo; Exc: {exc}", file=sys.stderr)
        data["ram_scrape_error"] = repr(exc)

    data["hard_disk"] = {}
    try:
        disk_usage = shutil.disk_usage(".")
        data["hard_disk"] = {
            "total": disk_usage.total // 1024,  # in kiB
            "used": disk_usage.used // 1024,
            "free": disk_usage.free // 1024,
        }
    except Exception as exc:
        # print(f"Error getting disk_usage from shutil: {exc}", file=sys.stderr)
        data["hard_disk_scrape_error"] = repr(exc)

    data["os"] = ""
    try:
        data["os"] = run_cmd('lsb_release -d | grep -Po "Description:\\s*\\K.*"').strip()
    except Exception as exc:
        # print(f'Error getting os specs: {exc}', flush=True)
        data["os_scrape_error"] = repr(exc)

    return MachineSpecs(specs=data)


@asynccontextmanager
async def docker_container_wrapper(
    image: str,
    name: str = None,
    command: list[str] = None,
    clean_exit_timeout: float = 1.0,
    auto_remove: bool = False,
    **container_kwargs,
):
    """
    Context manager for Docker containers using Docker SDK.
    Creates and runs a container in a separate thread, yields it for interaction, and closes the client after the context exits.

    Parameters:
        image: Docker image to run
        name: Name of the container (default: None)
        command: Command to execute in the container. This should be formatted as a list that the
            Docker API would understand, e.g. ["bash", "-c", "..."]. If None, will run the default
            command for the image (default: None)
        clean_exit_timeout: Seconds to wait before force kill (default: 1.0)
        auto_remove: Automatically remove the container on exit. Equivalent to --rm flag (default: False)
        **container_kwargs: Additional keyword arguments passed to aiodocker.Docker.containers.create()
    """
    client = aiodocker.Docker()
    container = None

    # Configure and run the Docker container
    config = {"Image": image, **container_kwargs}
    if command:
        config["Cmd"] = command
    container = await client.containers.create(config=config, name=name)
    await container.start()

    try:
        yield container
    finally:
        if container and auto_remove:
            try:
                # Try to stop the container (with SIGTERM and then after timeout with SIGKILL)
                try:
                    await container.stop(timeout=int(clean_exit_timeout))
                except Exception as e:
                    logger.warning(f"Failed to stop container: {e}")

                # Remove the container
                try:
                    await container.delete(force=True)
                except Exception as e:
                    logger.warning(f"Failed to remove container: {e}")

            except Exception as e:
                logger.error(f"Failed to clean up container: {e}", exc_info=True)

        # Close the Docker client
        try:
            await client.close()
        except Exception as e:
            logger.warning(f"Failed to close Docker client: {e}")


async def get_docker_container_outputs(container: aiodocker.containers.DockerContainer):
    stdout = "".join(await container.log(stdout=True, stderr=False))
    stderr = "".join(await container.log(stdout=False, stderr=True))
    return stdout, stderr
