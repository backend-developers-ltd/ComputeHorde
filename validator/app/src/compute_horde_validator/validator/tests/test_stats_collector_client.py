import pytest
import responses
from django.conf import settings

from compute_horde_validator.validator.models import SystemEvent
from compute_horde_validator.validator.stats_collector_client import (
    StatsCollectorClient,
    StatsCollectorError,
)
from compute_horde_validator.validator.tests.helpers import get_keypair

STATS_COLLECTOR_URL = "http://localhost:1234/"


def create_system_events() -> list[SystemEvent]:
    events = SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
    return [
        events.create(
            type=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            subtype=SystemEvent.EventSubType.SUCCESS,
            data={},
            sent=False,
        ),
        events.create(
            type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
            data={},
            sent=False,
        ),
        events.create(
            type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
            data={},
            sent=True,
        ),
    ]


@pytest.mark.django_db(databases=["default", "default_alias"])
@responses.activate
def test_send_events_ok():
    hotkey = get_keypair().ss58_address
    responses.add(
        responses.POST, f"{STATS_COLLECTOR_URL}validator/{hotkey}/system_events", status=201
    )
    client = StatsCollectorClient(STATS_COLLECTOR_URL)
    events = create_system_events()
    client.send_events(events)


@pytest.mark.django_db(databases=["default", "default_alias"])
@responses.activate
def test_send_events_unauthenticated():
    hotkey = get_keypair().ss58_address
    responses.add(
        responses.POST, f"{STATS_COLLECTOR_URL}validator/{hotkey}/system_events", status=401
    )
    client = StatsCollectorClient(STATS_COLLECTOR_URL)
    events = create_system_events()
    with pytest.raises(StatsCollectorError):
        client.send_events(events)


@pytest.mark.django_db(databases=["default", "default_alias"])
@responses.activate
def test_send_events_bad_request():
    hotkey = get_keypair().ss58_address
    responses.add(
        responses.POST, f"{STATS_COLLECTOR_URL}validator/{hotkey}/system_events", status=400
    )
    client = StatsCollectorClient(STATS_COLLECTOR_URL)
    events = create_system_events()
    with pytest.raises(StatsCollectorError):
        client.send_events(events)


@pytest.mark.django_db(databases=["default", "default_alias"])
@responses.activate
def test_get_sentry_dsn_ok():
    hotkey = get_keypair().ss58_address
    responses.add(
        responses.GET,
        f"{STATS_COLLECTOR_URL}validator/{hotkey}/sentry_dsn",
        json={"sentry_dsn": "https://public@sentry.example.com/1"},
    )
    client = StatsCollectorClient(STATS_COLLECTOR_URL)
    dsn = client.get_sentry_dsn()
    assert dsn == "https://public@sentry.example.com/1"


@pytest.mark.django_db(databases=["default", "default_alias"])
@responses.activate
def test_get_sentry_dsn_unauthenticated():
    hotkey = get_keypair().ss58_address
    responses.add(responses.GET, f"{STATS_COLLECTOR_URL}validator/{hotkey}/sentry_dsn", status=401)
    client = StatsCollectorClient(STATS_COLLECTOR_URL)
    with pytest.raises(StatsCollectorError):
        client.get_sentry_dsn()


@pytest.mark.django_db(databases=["default", "default_alias"])
@responses.activate
def test_get_sentry_dsn_not_found():
    hotkey = get_keypair().ss58_address
    responses.add(
        responses.GET,
        f"{STATS_COLLECTOR_URL}validator/{hotkey}/sentry_dsn",
        json={"sentry_dsn": None},
    )
    client = StatsCollectorClient(STATS_COLLECTOR_URL)
    dsn = client.get_sentry_dsn()
    assert dsn is None


@pytest.mark.django_db(databases=["default", "default_alias"])
@responses.activate
def test_get_sentry_dsn_invalid_response():
    hotkey = get_keypair().ss58_address
    responses.add(
        responses.GET,
        f"{STATS_COLLECTOR_URL}validator/{hotkey}/sentry_dsn",
        json={"foo": "bar"},
        status=200,
    )
    client = StatsCollectorClient(STATS_COLLECTOR_URL)
    with pytest.raises(StatsCollectorError):
        client.get_sentry_dsn()
