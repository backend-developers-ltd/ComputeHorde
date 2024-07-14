import factory
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS

from compute_horde_miner.miner.models import JobFinishedReceipt, JobStartedReceipt


class AbstractReceiptFactory(factory.django.DjangoModelFactory):
    class Meta:
        abstract = True

    job_uuid = factory.Faker("uuid4")
    miner_hotkey = factory.Faker("uuid4")
    validator_hotkey = factory.Faker("uuid4")

    validator_signature = factory.Faker("word")
    miner_signature = factory.Faker("word")


class JobStartedReceiptFactory(AbstractReceiptFactory):
    class Meta:
        model = JobStartedReceipt

    executor_class = DEFAULT_EXECUTOR_CLASS
    time_accepted = factory.Faker("date_time")
    max_timeout = 100


class JobFinishedReceiptFactory(AbstractReceiptFactory):
    class Meta:
        model = JobFinishedReceipt

    time_started = factory.Faker("date_time")
    time_took_us = factory.Faker("pyint", min_value=0, max_value=1000)
    score_str = factory.Faker("word")
