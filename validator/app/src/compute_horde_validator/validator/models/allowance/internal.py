"""Models internal for the allowance module, not to be used by other modules."""

from compute_horde_core.executor_class import ExecutorClass
from django.db import models
from django.db.models import UniqueConstraint


class MinerAddress(models.Model):
    hotkey_ss58address = models.TextField(unique=True)
    address = models.TextField()
    port = models.IntegerField()


class Neuron(models.Model):
    hotkey_ss58address = models.TextField()
    coldkey_ss58address = models.TextField()
    block = models.IntegerField()


class AllowanceMinerManifest(models.Model):
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


class AllowanceBooking(models.Model):
    creation_timestamp = models.DateTimeField(auto_now_add=True)
    is_reserved = models.BooleanField()
    is_spent = models.BooleanField()
    reservation_expiry_time = models.DateTimeField(null=True, blank=True)


class Block(models.Model):
    block_number = models.BigIntegerField(primary_key=True)
    creation_timestamp = models.DateTimeField()
    end_timestamp = models.DateTimeField(
        null=True,
        blank=True,
        help_text="The timestamp of the block's end (next blocks creation timestamp). Blocks with field set are "
        "considered to have all the allowance calculated",
    )


class BlockAllowance(models.Model):
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
