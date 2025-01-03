import json
import logging
import time
from typing import TYPE_CHECKING, Any

import pydantic
import requests
from django.conf import settings

if TYPE_CHECKING:
    from compute_horde_validator.validator.models import SystemEvent

logger = logging.getLogger(__name__)


class ValidatorSentryDSNResponse(pydantic.BaseModel):
    sentry_dsn: str | None


def get_keypair():
    return settings.BITTENSOR_WALLET().get_hotkey()


class StatsCollectorError(RuntimeError):
    @property
    def response(self) -> requests.Response | None:
        if isinstance(self.__cause__, requests.RequestException):
            return self.__cause__.response
        return None


class StatsCollectorClient:
    def __init__(self, url: str = settings.STATS_COLLECTOR_URL):
        if not url.endswith("/"):
            url += "/"
        self.url = url
        self.keypair = get_keypair()
        self.timeout = 5  # seconds

    def send_events(self, events: list["SystemEvent"]) -> None:
        hotkey = self.keypair.ss58_address
        data = [event.to_dict() for event in events]
        path = f"validator/{hotkey}/system_events"

        self.make_request("POST", path, data=data)

    def get_sentry_dsn(self) -> str | None:
        hotkey = self.keypair.ss58_address

        response = self.make_request("GET", f"validator/{hotkey}/sentry_dsn")

        try:
            model = ValidatorSentryDSNResponse.model_validate_json(response)
        except pydantic.ValidationError as e:
            raise StatsCollectorError("Malformed response") from e

        return model.sentry_dsn

    def make_request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        data: Any = None,
        **kwargs,
    ) -> str:
        url = self.url + path
        logger.info("%s %s", method, url)
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
            raise StatsCollectorError(repr(e)) from e

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
