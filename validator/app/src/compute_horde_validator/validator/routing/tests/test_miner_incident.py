import uuid

import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.models import MinerIncident
from compute_horde_validator.validator.routing.default import routing
from compute_horde_validator.validator.routing.types import MinerIncidentType


@pytest.mark.django_db(transaction=True)
def test_report_miner_incident_records_single_incident():
    """A single incident is persisted with correct field values."""
    hotkey = "miner_hotkey_single"
    job_uuid = str(uuid.uuid4())
    incident_type = MinerIncidentType.MINER_JOB_REJECTED
    executor_class = ExecutorClass.always_on__gpu_24gb

    routing().report_miner_incident(
        type=incident_type,
        hotkey_ss58address=hotkey,
        job_uuid=job_uuid,
        executor_class=executor_class,
    )

    incidents = [inc for inc in MinerIncident.objects.filter(hotkey_ss58address=hotkey)]
    assert len(incidents) == 1
    incident = incidents[0]
    assert incident.type == incident_type
    assert str(incident.job_uuid) == job_uuid
    assert incident.hotkey_ss58address == hotkey
    assert incident.executor_class == executor_class.value


@pytest.mark.django_db(transaction=True)
def test_report_miner_incident_multiple_same_miner():
    """Multiple incidents for the same miner are stored independently."""
    hotkey = "miner_hotkey_multi"
    executor_class = ExecutorClass.always_on__gpu_24gb

    first_uuid = str(uuid.uuid4())
    second_uuid = str(uuid.uuid4())

    routing().report_miner_incident(
        type=MinerIncidentType.MINER_JOB_REJECTED,
        hotkey_ss58address=hotkey,
        job_uuid=first_uuid,
        executor_class=executor_class,
    )
    routing().report_miner_incident(
        type=MinerIncidentType.MINER_JOB_FAILED,
        hotkey_ss58address=hotkey,
        job_uuid=second_uuid,
        executor_class=executor_class,
    )

    incidents = [
        inc for inc in MinerIncident.objects.filter(hotkey_ss58address=hotkey).order_by("timestamp")
    ]
    assert len(incidents) == 2
    types = {i.type for i in incidents}
    assert types == {
        MinerIncidentType.MINER_JOB_REJECTED,
        MinerIncidentType.MINER_JOB_FAILED,
    }


@pytest.mark.django_db(transaction=True)
def test_report_miner_incident_different_miners():
    hotkey1 = "miner_hotkey_A"
    hotkey2 = "miner_hotkey_B"
    executor_class = ExecutorClass.always_on__gpu_24gb

    routing().report_miner_incident(
        type=MinerIncidentType.MINER_JOB_REJECTED,
        hotkey_ss58address=hotkey1,
        job_uuid=str(uuid.uuid4()),
        executor_class=executor_class,
    )
    routing().report_miner_incident(
        type=MinerIncidentType.MINER_JOB_FAILED,
        hotkey_ss58address=hotkey2,
        job_uuid=str(uuid.uuid4()),
        executor_class=executor_class,
    )

    inc1 = [inc for inc in MinerIncident.objects.filter(hotkey_ss58address=hotkey1)]
    inc2 = [inc for inc in MinerIncident.objects.filter(hotkey_ss58address=hotkey2)]
    assert len(inc1) == 1
    assert len(inc2) == 1
    assert inc1[0].type == MinerIncidentType.MINER_JOB_REJECTED
    assert inc2[0].type == MinerIncidentType.MINER_JOB_FAILED


@pytest.mark.django_db(transaction=True)
def test_report_miner_incident_all_enum_types():
    """All enum types can be recorded; multiple incidents under one miner hotkey accumulate."""
    executor_class = ExecutorClass.always_on__gpu_24gb
    hotkey = "miner_hotkey_all_enum"

    for incident_type in MinerIncidentType:
        routing().report_miner_incident(
            type=incident_type,
            hotkey_ss58address=hotkey,
            job_uuid=str(uuid.uuid4()),
            executor_class=executor_class,
        )

    incidents = [inc for inc in MinerIncident.objects.filter(hotkey_ss58address=hotkey)]
    assert len(incidents) == len(MinerIncidentType)
    recorded_types = {i.type for i in incidents}
    assert recorded_types == {t.value for t in MinerIncidentType}
