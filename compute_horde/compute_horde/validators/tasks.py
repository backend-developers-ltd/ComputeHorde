import logging

from celery import shared_task
from django.apps import apps
from django.conf import settings

from compute_horde.utils import get_validators

logger = logging.getLogger(__name__)


@shared_task()
def fetch_validators():
    Validator = apps.get_model(settings.COMPUTE_HORDE_VALIDATOR_MODEL)
    key_field = settings.COMPUTE_HORDE_VALIDATOR_KEY_FIELD
    active_field = settings.COMPUTE_HORDE_VALIDATOR_ACTIVE_FIELD

    debug_validator_keys = set()
    if debug_field := getattr(settings, "COMPUTE_HORDE_VALIDATOR_DEBUG_FIELD", None):
        debug_validator_keys = set(
            Validator.objects.filter(**{debug_field: True, active_field: True}).values_list(
                key_field, flat=True
            )
        )

    validators = get_validators(
        netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK
    )
    validator_keys = {v.hotkey for v in validators} | debug_validator_keys

    to_activate = []
    to_deactivate = []
    to_create = []
    for validator in Validator.objects.all():
        public_key = getattr(validator, key_field)
        if public_key in validator_keys:
            to_activate.append(validator)
            setattr(validator, active_field, True)
            validator_keys.remove(public_key)
        else:
            setattr(validator, active_field, False)
            to_deactivate.append(validator)

    for key in validator_keys:
        to_create.append(Validator(**{key_field: key, active_field: True}))

    Validator.objects.bulk_create(to_create)
    Validator.objects.bulk_update(to_activate + to_deactivate, [active_field])
    logger.info(
        f"Fetched validators. Activated: {len(to_activate)}, deactivated: {len(to_deactivate)}, "
        f"created: {len(to_create)}"
    )
