from datetime import timedelta

from compute_horde_core.executor_class import ExecutorClass
from django.db import transaction
from django.utils import timezone

from ...models.allowance.internal import AllowanceBooking, BlockAllowance
from .. import settings
from ..metrics import (
    VALIDATOR_RESERVE_ALLOWANCE_DURATION,
    VALIDATOR_UNDO_ALLOWANCE_RESERVATION_DURATION,
    timing_decorator,
)
from ..types import (
    AllowanceException,
    CannotReserveAllowanceException,
    ReservationAlreadySpent,
    ReservationNotFound,
    block_ids,
    reservation_id,
    ss58_address,
)


@timing_decorator(VALIDATOR_RESERVE_ALLOWANCE_DURATION)
def reserve_allowance(
    miner: ss58_address,
    validator: ss58_address,
    executor_class: ExecutorClass,
    allowance_seconds: float,
    job_start_block: int,
) -> tuple[reservation_id, block_ids]:
    """
    Reserve allowance for a specific miner. The reservation will auto expire after
    `amount + settings.RESERVATION_MARGIN_SECONDS` seconds.

    This is used for temporary allowance reservation for pending jobs.

    Args:
        miner: hotkey of the miner
        validator: hotkey of the validator
        executor_class: When the reservation expires
        allowance_seconds: Amount of allowance to reserve (in seconds)
        job_start_block: used to determine which blocks can be used for the reservation, as per block expiry rules

    Returns:
        Id of the reservation, which can be used to cancel the reservation, or make it a permanent spend.

    raises CannotReserveAllowanceException if there is not enough allowance from the miner.
    """
    if allowance_seconds > settings.MAX_JOB_RUN_TIME:
        raise AllowanceException(
            f"Required allowance cannot be greater than {settings.MAX_JOB_RUN_TIME} seconds"
        )

    # Calculate the earliest usable block based on block expiry rules
    earliest_usable_block = job_start_block - settings.BLOCK_EXPIRY

    with transaction.atomic():
        # select_for_update lock ordering and deadlock risk.
        #
        # This function locks BlockAllowance rows first, then (if needed) related AllowanceBooking rows.
        # undo_allowance_reservation/spend_allowance lock AllowanceBooking first then BlockAllowance.
        # The inconsistent locking order is a potential deadlock scenario.
        #
        # Why is this safe?
        #
        # Reserve only ever locks bookings that are ALREADY referenced by the
        # locked BlockAllowance rows (never an arbitrary different booking). Thus the lock order for
        # any overlapping set is effectively: (other tx) booking -> allowances  vs  (this tx) allowances -> that same booking.
        # For a deadlock cycle you'd need TxA waiting on a lock held by TxB while TxB waits on a lock held by TxA.
        # Here, after we lock allowances we may try to lock their booking. Another transaction that locked the booking
        # first will then attempt to lock THE SAME allowances (to modify / undo / spend) but it cannot obtain them
        # until we release; it won't hold any allowance locks we need. Therefore no circular wait arises.
        #
        # IMPORTANT: If future logic broadens the scope (e.g., reserve inspects unrelated bookings) we MUST revisit this.

        # Get available allowances for the specific miner
        available_block_allowances = (
            BlockAllowance.objects.select_for_update()
            .filter(
                miner_ss58=miner,
                validator_ss58=validator,
                executor_class=executor_class.value,
                block__block_number__gte=earliest_usable_block,
                invalidated_at_block__isnull=True,  # Only non-invalidated allowances
            )
            .filter(
                # Available allowance: booking is null
                allowance_booking__isnull=True
            )
            .select_related("block")
            .order_by("block__block_number")
        )  # Order by block number for consistent selection

        # Calculate total available allowance
        # We compute it in Python instead of using Django `aggregate()`, that executes its own SQL query.
        # and can return different rows than the ones we locked above, because Django can optimize away
        # the `select_for_update()` in the aggregate query.
        total_available = sum(
            block_allowance.allowance for block_allowance in available_block_allowances
        )

        # Check if there's enough allowance
        if total_available < allowance_seconds:
            raise CannotReserveAllowanceException(
                miner=miner,
                required_allowance_seconds=allowance_seconds,
                available_allowance_seconds=total_available,
            )

        # Create the reservation booking
        expiry_time = timezone.now() + timedelta(
            seconds=allowance_seconds + settings.RESERVATION_MARGIN_SECONDS
        )
        booking = AllowanceBooking.objects.create(
            is_reserved=True, is_spent=False, reservation_expiry_time=expiry_time
        )

        # Reserve allowances up to the required amount
        reserved_amount = 0.0
        reserved_block_ids = []
        allowances_to_update = []

        for block_allowance in available_block_allowances:
            if reserved_amount >= allowance_seconds:
                break

            # Link this allowance to the booking
            block_allowance.allowance_booking = booking
            allowances_to_update.append(block_allowance)

            reserved_amount += block_allowance.allowance
            reserved_block_ids.append(block_allowance.block.block_number)

        # Bulk update all allowances at once
        if allowances_to_update:
            BlockAllowance.objects.bulk_update(allowances_to_update, ["allowance_booking"])

        return booking.id, reserved_block_ids


@timing_decorator(VALIDATOR_UNDO_ALLOWANCE_RESERVATION_DURATION)
def undo_allowance_reservation(reservation_id_: reservation_id) -> None:
    """
    Undo a previously made allowance reservation.

    This releases the reserved allowance back to the available pool.

    raises ReservationNotFound if the reservation is not found.
    """
    with transaction.atomic():
        try:
            # Find the booking by reservation ID
            booking = AllowanceBooking.objects.select_for_update().get(id=reservation_id_)
        except AllowanceBooking.DoesNotExist:
            raise ReservationNotFound(f"Reservation with ID {reservation_id_} not found")

        # Spent reservations can't be undone
        if booking.is_spent:
            raise ReservationAlreadySpent(f"Reservation with ID {reservation_id_} is already spent")

        # Find all BlockAllowances associated with this booking
        associated_allowances = BlockAllowance.objects.select_for_update().filter(
            allowance_booking=booking
        )

        # Release the allowances back to the available pool
        allowances_to_update = []
        for allowance in associated_allowances:
            allowance.allowance_booking = None
            allowances_to_update.append(allowance)

        # Bulk update all allowances
        if allowances_to_update:
            BlockAllowance.objects.bulk_update(allowances_to_update, ["allowance_booking"])

        # Delete the booking record
        booking.delete()


def spend_allowance(reservation_id_: reservation_id) -> None:
    """
    Spend allowance (make a reservation permanent).

     Args:
         reservation_id_: reservation_id

     raises ReservationNotFound if the reservation is not found. raise ReservationAlreadySpent is reservation
     already spent.
    """
    with transaction.atomic():
        updated_count = (
            AllowanceBooking.objects.select_for_update()
            .filter(id=reservation_id_, is_spent=False)
            .update(is_spent=True, is_reserved=False, reservation_expiry_time=None)
        )

        if updated_count == 0:
            # Check if the reservation exists but is already spent
            if AllowanceBooking.objects.filter(id=reservation_id_, is_spent=True).exists():
                raise ReservationAlreadySpent(
                    f"Reservation with ID {reservation_id_} is already spent"
                )
            else:
                raise ReservationNotFound(f"Reservation with ID {reservation_id_} not found")
