import pytest

from compute_horde_miner.miner.models import JobStartedReceipt
from compute_horde_miner.miner.tests import factories

pytestmark = [pytest.mark.django_db]


def test_active_job_started_receipts(django_assert_num_queries):
    started_receipts_active = factories.JobStartedReceiptFactory.create_batch(3)

    started_receipts_with_finished = factories.JobStartedReceiptFactory.create_batch(3)

    for receipt in started_receipts_with_finished:
        factories.JobFinishedReceiptFactory(job_uuid=receipt.job_uuid)

    with django_assert_num_queries(1):
        result = list(JobStartedReceipt.objects.get_active())

    assert set(result) == set(started_receipts_active)
