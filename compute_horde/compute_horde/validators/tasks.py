import logging

from celery import shared_task
from django.apps import apps
from django.conf import settings

from compute_horde.utils import get_validators

logger = logging.getLogger(__name__)


@shared_task()
def fetch_validators():
    Validator = apps.get_model(settings.VALIDATOR_MODEL)

    debug_validator_keys = set()
    if debug_field := getattr(settings, "VALIDATOR_DEBUG_FIELD", None):
        debug_validator_keys = set(
            Validator.objects.filter(
                **{
                    debug_field: True,
                    settings.VALIDATOR_ACTIVE_FIELD: True,
                }
            ).values_list(settings.VALIDATOR_KEY_FIELD, flat=True)
        )

    validators = get_validators(
        netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK
    )
    validator_keys = {v.hotkey for v in validators} | debug_validator_keys

    to_activate = []
    to_deactivate = []
    to_create = []
    for validator in Validator.objects.all():
        if validator.public_key in validator_keys:
            to_activate.append(validator)
            validator.active = True
            validator_keys.remove(validator.public_key)
        else:
            validator.active = False
            to_deactivate.append(validator)

    for key in validator_keys:
        to_create.append(
            Validator(
                **{
                    settings.VALIDATOR_KEY_FIELD: key,
                    settings.VALIDATOR_ACTIVE_FIELD: True,
                }
            )
        )

    Validator.objects.bulk_create(to_create)
    Validator.objects.bulk_update(to_activate + to_deactivate, [settings.VALIDATOR_ACTIVE_FIELD])
    logger.info(
        f"Fetched validators. Activated: {len(to_activate)}, deactivated: {len(to_deactivate)}, "
        f"created: {len(to_create)}"
    )
