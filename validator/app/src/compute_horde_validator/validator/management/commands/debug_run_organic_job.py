import argparse
import asyncio
import json
import logging

import bittensor
from compute_horde.mv_protocol.miner_requests import (
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.models import Miner, OrganicJob
from compute_horde_validator.validator.synthetic_jobs.generator.cli import CLIJobGenerator
from compute_horde_validator.validator.synthetic_jobs.utils import (
    _execute_job,
)

logger = logging.getLogger(__name__)


def string_list(value):
    values = json.loads(value)
    if not isinstance(values, list):
        raise argparse.ArgumentTypeError(f"{value} is not a list of strings")
    if not all(isinstance(v, str) for v in values):
        raise argparse.ArgumentTypeError(f"{value} is not a list of strings")
    return values


class Command(BaseCommand):
    """
    For running in dev environment, not in production
    """

    def add_arguments(self, parser):
        parser.add_argument("--miner_uid", type=int, help="Miner uid", required=True)
        parser.add_argument("--timeout", type=int, help="Timeout value", required=True)
        parser.add_argument("--base_docker_image_name", type=str, help="First string argument", required=True)
        parser.add_argument("--docker_image_name", type=str, help="Second string argument", required=True)

        parser.add_argument(
            "--docker_run_options_preset",
            type=str,
            help="executors translate this to the 'RUN_OPTS' part of 'docker run *RUN_OPTS image_name *RUN_CMD'",
            required=True,
        )
        parser.add_argument(
            "--docker_run_cmd",
            type=string_list,
            help="the 'RUN_CMD' part of 'docker run *RUN_OPTS image_name *RUN_CMD'",
            required=True,
        )

    def handle(self, *args, **options):
        miner_uid = options["miner_uid"]
        timeout = options["timeout"]
        base_docker_image_name = options["base_docker_image_name"]
        docker_image_name = options["docker_image_name"]
        docker_run_options_preset = options["docker_run_options_preset"]
        docker_run_cmd = options["docker_run_cmd"]
        CLIJobGenerator.set_parameters(
            timeout=timeout,
            base_docker_image_name=base_docker_image_name,
            docker_image_name=docker_image_name,
            docker_run_options_preset=docker_run_options_preset,
            docker_run_cmd=docker_run_cmd,
        )
        metagraph = bittensor.metagraph(settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK)
        neurons = [n for n in metagraph.neurons if n.uid == miner_uid]
        if not neurons:
            raise ValueError(f"{miner_uid=} not present in this subnetowrk")
        neuron = neurons[0]
        if not neuron.axon_info.is_serving:
            raise ValueError(f"{miner_uid=} did not announce it's ip address")

        job = OrganicJob.objects.create(
            miner=Miner.objects.get_or_create(hotkey=neuron.hotkey)[0],
            miner_address=neuron.axon_info.ip,
            miner_address_ip_version=neuron.axon_info.ip_type,
            miner_port=neuron.axon_info.port,
        )
        _, msg = asyncio.run(_execute_job(job))
        if isinstance(msg, V0DeclineJobRequest):
            print("Miner declined")
            raise SystemExit(1)
        elif isinstance(msg, V0ExecutorFailedRequest):
            print("Miner accepted but executor failed to prepare")
            raise SystemExit(1)
        elif isinstance(msg, V0JobFailedRequest):
            print("Executor started the job but failed")
            exit_status = 1
        elif isinstance(msg, V0JobFinishedRequest):
            print("Executor finished the job successfully")
            exit_status = 0
        else:
            raise ValueError(f"Unexpected message: {msg}")
        print(f"stderr: {msg.docker_process_stderr}")
        print(f"\nstdout: {msg.docker_process_stdout}")
        raise SystemExit(exit_status)
