import json
import time

import bittensor
import pydantic
import requests
from django.conf import settings

from compute_horde_validator.validator.models import SystemEvent


class ValidatorSentryDSNResponse(pydantic.BaseModel):
    sentry_dsn: str


def get_keypair():
    return settings.BITTENSOR_WALLET().get_hotkey()


class StatsCollectorError(RuntimeError):
    @property
    def response(self) -> requests.Response | None:
        if isinstance(self.__cause__, requests.RequestException):
            return self.__cause__.response
        return None


class StatsCollectorClient:
    def __init__(
        self,
        url: str = settings.STATS_COLLECTOR_URL,
        keypair: bittensor.Keypair | None = None,
        timeout: float = 5,
    ):
        self.url = url
        self.keypair = keypair or get_keypair()
        self.timeout = timeout

    def send_events(self, events: list[SystemEvent]) -> None:
        hotkey = self.keypair.ss58_address
        data = [event.to_dict() for event in events]
        path = f"validator/{hotkey}/system_events"
        self.make_request("POST", path, data=data)

    def get_sentry_dsn(self) -> str | None:
        hotkey = self.keypair.ss58_address
        try:
            response = self.make_request("GET", f"validator/{hotkey}/sentry_dsn")
        except StatsCollectorError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise e

        return ValidatorSentryDSNResponse.model_validate_json(response).sentry_dsn

    def make_request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        data: dict | list | None = None,
        **kwargs,
    ) -> str:
        try:
            response = requests.request(
                method,
                self.url + path,
                json=data,
                headers={**self.get_signature_headers(), **(headers or {})},
                timeout=self.timeout,
                **kwargs,
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            raise StatsCollectorError() from e

    def get_signature_headers(self):
        hotkey = self.keypair.ss58_address
        signing_timestamp = int(time.time())
        to_sign = json.dumps(
            {"signing_timestamp": signing_timestamp, "validator_ss58_address": hotkey},
            sort_keys=True,
        )
        signature = f"0x{self.keypair.sign(to_sign).hex()}"
        return {
            "Validator-Signature": signature,
            "Validator-Signing-Timestamp": str(signing_timestamp),
        }
