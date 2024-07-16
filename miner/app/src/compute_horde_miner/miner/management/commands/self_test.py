import argparse
import secrets
import subprocess
from pathlib import Path

from django.conf import settings
from django.core.management import BaseCommand
from django.db import connection
from pydantic import PostgresDsn

from compute_horde_miner.miner.models import Validator


class Command(BaseCommand):
    """
    Run self tests by emulating validator synthetic job requests
    """

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--weights_version",
            default=None,
            type=int,
            help="Override weights version for synthetic jobs",
        )
        parser.add_argument(
            "--validator_db_name",
            default="validator",
            type=str,
            help="Database name for validator container",
        )
        parser.add_argument(
            "--validator_env_file",
            default=Path(".env.validator"),
            type=Path,
            help="Env file name for validator container (default: .env.validator)",
        )
        parser.add_argument(
            "--validator_image",
            default="andreeareef/compute-horde-validator:v0-latest",
            type=str,
            help=argparse.SUPPRESS,
        )
        parser.add_argument(
            "--clean_validator_records",
            default=True,
            type=bool,
            help=argparse.SUPPRESS,
        )
        parser.add_argument(
            "--remove_env_file",
            default=True,
            type=bool,
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        wallet = settings.BITTENSOR_WALLET()
        hotkey_address = wallet.hotkey.ss58_address

        create_env_file(
            env_file_path=options["validator_env_file"],
            db_name=options["validator_db_name"],
            hotkey_address=hotkey_address,
            weights_version=options["weights_version"],
        )

        create_validator_database(options["validator_db_name"])

        Validator.objects.update_or_create(
            public_key=hotkey_address,
            debug=True,
            defaults={"active": True},
        )

        try:
            run_validator_synthetic_jobs(
                validator_image=options["validator_image"], env_file=options["validator_env_file"]
            )
        finally:
            if options["remove_env_file"]:
                options["validator_env_file"].unlink()
            if options["clean_validator_records"]:
                Validator.objects.filter(public_key=hotkey_address, debug=True).delete()


def run_validator_synthetic_jobs(*, validator_image: str, env_file: Path):
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-it",
            "--network=host",
            "--add-host=host.docker.internal:host-gateway",
            "--env-file",
            str(env_file),
            "-v",
            f"{settings.BITTENSOR_WALLET_DIRECTORY}:/root/.bittensor/wallets",
            validator_image,
            "/bin/sh",
            "-c",
            "python manage.py migrate && python manage.py debug_run_synthetic_jobs",
        ]
    )


def create_validator_database(db_name: str):
    with connection.cursor() as cursor:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name};")
        cursor.execute(
            f"CREATE DATABASE {db_name} WITH OWNER = {settings.DATABASES['default']['USER']}",
        )


def create_env_file(
    *, env_file_path: Path, db_name: str, hotkey_address: str, weights_version: int | None = None
):
    secret_key = secrets.token_urlsafe(32)
    database_url = PostgresDsn.build(
        scheme="postgresql",
        host=settings.DATABASES["default"]["HOST"],
        port=settings.DATABASES["default"]["PORT"],
        username=settings.DATABASES["default"]["USER"],
        password=settings.DATABASES["default"]["PASSWORD"],
        path=db_name,
    )

    content = f"""SECRET_KEY={secret_key}
POSTGRES_PASSWORD={settings.DATABASES["default"]["PASSWORD"]}
DATABASE_URL={database_url}
BITTENSOR_NETWORK={settings.BITTENSOR_NETWORK}
BITTENSOR_NETUID={settings.BITTENSOR_NETUID}
BITTENSOR_WALLET_NAME={settings.BITTENSOR_WALLET_NAME}
BITTENSOR_WALLET_HOTKEY_NAME={settings.BITTENSOR_WALLET_HOTKEY_NAME}
DEBUG_MINER_KEY={hotkey_address}
DEBUG_MINER_ADDRESS=host.docker.internal
DEBUG_MINER_PORT=8000
"""

    if weights_version is not None:
        content += f"DEBUG_OVERRIDE_WEIGHTS_VERSION={weights_version}\n"

    env_file_path.write_text(content)
