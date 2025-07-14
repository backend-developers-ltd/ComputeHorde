import logging

from django.conf import settings
from django.core.management import BaseCommand

from compute_horde.smart_contracts.map_contract import (
    get_dynamic_configs_from_contract,
    get_dynamic_config_types_from_settings,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Fetch dynamic configuration from Map smart contract"

    def handle(self, *args, **options):
        if not settings.USE_CONTRACT_CONFIG:
            self.stdout.write(
                self.style.WARNING("Fetch dynamic config from contract feature flag is disabled")
            )
            return

        self._fetch_and_display_configs()

    def _fetch_and_display_configs(self) -> None:
        contract_address = settings.CONFIG_CONTRACT_ADDRESS
        self.stdout.write(f"Fetching dynamic config from contract: {contract_address}")

        try:
            dynamic_config_keys = get_dynamic_config_types_from_settings()

            if not dynamic_config_keys:
                self.stdout.write(self.style.WARNING("No dynamic config keys found in settings"))
                return

            configs = get_dynamic_configs_from_contract(
                dynamic_config_keys,
                contract_address,
            )

            if not configs:
                self.stdout.write(self.style.WARNING("No dynamic config found in contract"))
                return

            for key, value in configs.items():
                if value is not None:
                    self.stdout.write(f"✓ {key}: {value}")
                else:
                    self.stdout.write(self.style.WARNING(f"✗ {key}: Not found"))

        except Exception as exc:
            error_msg = f"Failed to fetch dynamic config from contract: {exc}"
            self.stdout.write(self.style.ERROR(error_msg))
