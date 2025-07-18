import json
from argparse import ArgumentParser

from django.conf import settings
from django.core.management import BaseCommand

from compute_horde.smart_contracts import map_contract


class Command(BaseCommand):
    help = "Fetch and list all dynamic config values from the smart contract"

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--contract-address",
            help="Smart contract address to fetch config from",
            default=getattr(settings, "DYNAMIC_CONFIG_MAP_SMART_CONTRACT_ADDRESS", None),
        )
        parser.add_argument(
            "--key",
            help="Specific key to fetch from the contract. If not provided, all keys will be fetched",
            default=None,
        )
        parser.add_argument(
            "--format",
            choices=["json", "table"],
            default="table",
            help="Output format (default: table)",
        )

    def handle(self, *args, **options):
        contract_address = options.get("contract_address")
        if not contract_address:
            self.stdout.write(self.style.ERROR("Contract address must be provided."))
            return

        self.stdout.write(f"Dynamic config from contract: {contract_address}")

        if config_key := options.get("key"):
            try:
                value = map_contract.get_contract_value(contract_address, config_key)
                if options["format"] == "json":
                    self.stdout.write(json.dumps({config_key: value}, indent=2))
                else:
                    self.print_value(config_key, value)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error fetching dynamic config: {e}"))
            return

        try:
            dynamic_configs = map_contract.get_dynamic_configs_from_contract(contract_address)
            if options["format"] == "json":
                self.stdout.write(json.dumps(dynamic_configs, indent=2))
            else:
                self.stdout.write("-" * 50)

                if not dynamic_configs:
                    self.stdout.write("No dynamic config found")
                    return

                for key, value in dynamic_configs.items():
                    self.print_value(key, value)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error fetching dynamic config: {e}"))

    def print_value(self, key: str, value: str) -> None:
        status = "✓" if value is not None else "✗"
        value_str = str(value) if value is not None else "Not found"
        self.stdout.write(f"{status} {key}: {value_str}")
