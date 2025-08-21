"""Models internal for the allowance module, not to be used by other modules."""

from collections.abc import Iterable

from compute_horde_core.executor_class import ExecutorClass
from django.db import models
from django.db.models import UniqueConstraint
from django_prometheus.models import (
    ExportModelOperationsMixin,
    model_deletes,
    model_inserts,
    model_updates,
)


def BulkExportModelOperationsMixin(model_name):
    # TODO: put me in a better place. like django_prometheus.models.
    """
    Extend ExportModelOperationsMixin with bulk operation support.
    """

    class BulkExportQuerySet(models.QuerySet):  # type: ignore
        def bulk_create(self, objs: Iterable, *args, **kwargs):  # type: ignore
            ret = super().bulk_create(objs, *args, **kwargs)
            model_inserts.labels(model_name).inc(len(objs))  # type: ignore
            return ret

        def bulk_update(self, objs: Iterable, *args, **kwargs):  # type: ignore
            ret = super().bulk_update(objs, *args, **kwargs)
            model_updates.labels(model_name).inc(len(objs))  # type: ignore
            return ret

        def delete(self):
            deleted, _rows_count = super().delete()
            model_deletes.labels(model_name).inc(deleted)
            return deleted, _rows_count

    base_mixin_cls = ExportModelOperationsMixin(model_name)

    class Mixin(base_mixin_cls, models.Model):  # type: ignore
        class Meta:
            abstract = True

        objects = BulkExportQuerySet.as_manager()

    Mixin.__qualname__ = f"BulkExportModelOperationsMixin('{model_name}')"
    return Mixin


class MinerAddress(models.Model):
    hotkey_ss58address = models.TextField(unique=True)
    address = models.TextField()
    ip_version = models.IntegerField()
    port = models.IntegerField()


class Neuron(models.Model):
    hotkey_ss58address = models.TextField()
    coldkey_ss58address = models.TextField()
    block = models.IntegerField()


class AllowanceMinerManifest(  # type: ignore
    BulkExportModelOperationsMixin("AllowanceMinerManifest"),  # type: ignore
    models.Model,
):
    miner_ss58address = models.TextField()
    block_number = models.BigIntegerField()
    success = models.BooleanField(help_text="Whether the manifest was successfully retrieved")
    executor_class = models.CharField(
        max_length=255, choices=[(choice.value, choice.value) for choice in ExecutorClass]
    )
    is_drop = models.BooleanField(
        help_text="Whether the executor count is lower compared to the previously scraped value"
    )
    executor_count = models.IntegerField(
        default=0,
        help_text="The total number of available executors of this class as reported by the miner",
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["miner_ss58address", "block_number", "executor_class"],
                name="unique_allowance_miner_manifest",
            ),
        ]
        indexes = [
            models.Index(fields=["block_number"]),
            models.Index(fields=["miner_ss58address", "block_number"]),
        ]


class AllowanceBooking(BulkExportModelOperationsMixin("AllowanceBooking"), models.Model):  # type: ignore
    creation_timestamp = models.DateTimeField(auto_now_add=True)
    is_reserved = models.BooleanField()
    is_spent = models.BooleanField()
    reservation_expiry_time = models.DateTimeField(null=True, blank=True)


class Block(BulkExportModelOperationsMixin("Block"), models.Model):  # type: ignore
    block_number = models.BigIntegerField(primary_key=True)
    creation_timestamp = models.DateTimeField()
    end_timestamp = models.DateTimeField(
        null=True,
        blank=True,
        help_text="The timestamp of the block's end (next blocks creation timestamp). Blocks with field set are "
        "considered to have all the allowance calculated",
    )


class BlockAllowance(BulkExportModelOperationsMixin("BlockAllowance"), models.Model):  # type: ignore
    block = models.ForeignKey(Block, on_delete=models.CASCADE)
    allowance = models.FloatField()
    miner_ss58 = models.TextField()
    validator_ss58 = models.TextField()
    executor_class = models.CharField(
        max_length=255, choices=[(choice.value, choice.value) for choice in ExecutorClass]
    )
    invalidated_at_block = models.IntegerField(null=True, blank=True)
    allowance_booking = models.ForeignKey(
        AllowanceBooking, on_delete=models.CASCADE, null=True, blank=True
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["block", "miner_ss58", "validator_ss58", "executor_class"],
                name="unique_block_allowance",
            ),
        ]
