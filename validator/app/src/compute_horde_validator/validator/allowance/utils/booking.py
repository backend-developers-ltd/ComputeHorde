from datetime import timedelta
from django.db.models import Q, Sum
from django.utils import timezone

from compute_horde_core.executor_class import ExecutorClass
from ..types import ss58_address, reservation_id, block_ids, CannotReserveAllowanceException, ReservationNotFound
from ..models.internal import BlockAllowance, AllowanceBooking
from ..settings import BLOCK_EXPIRY, RESERVATION_MARGIN_SECONDS




def reserve_allowance(
        miner: ss58_address,
        validator: ss58_address,
        executor_class: ExecutorClass,
        amount: float,
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
        amount: Amount of allowance to reserve (in seconds)
        job_start_block: used to determine which blocks can be used for the reservation, as per block expiry rules

    Returns:
        Id of the reservation, which can be used to cancel the reservation, or make it a permanent spend.

    raises CannotReserveAllowanceException if there is not enough allowance from the miner.
    """
    # Calculate the earliest usable block based on block expiry rules
    earliest_usable_block = job_start_block - BLOCK_EXPIRY

    # Get available allowances for the specific miner
    available_allowances = BlockAllowance.objects.filter(
        miner_ss58=miner,
        validator_ss58=validator,
        executor_class=executor_class.value,
        block__block_number__gte=earliest_usable_block,
        invalidated_at_block__isnull=True,  # Only non-invalidated allowances
    ).filter(
        # Available allowance: booking is null OR (not spent AND not reserved)
        Q(allowance_booking__isnull=True) |
        Q(allowance_booking__is_spent=False, allowance_booking__is_reserved=False)
    ).select_related('block').order_by('block__block_number')  # Order by block number for consistent selection

    # Calculate total available allowance
    total_available = available_allowances.aggregate(
        total=Sum('allowance')
    )['total'] or 0.0

    # Check if there's enough allowance
    if total_available < amount:
        raise CannotReserveAllowanceException(
            f"Not enough allowance from miner {miner}. Required: {amount}, Available: {total_available}"
        )

    # Create the reservation booking
    expiry_time = timezone.now() + timedelta(seconds=amount + RESERVATION_MARGIN_SECONDS)
    booking = AllowanceBooking.objects.create(
        is_reserved=True,
        is_spent=False,
        reservation_expiry_time=expiry_time
    )

    # Reserve allowances up to the required amount
    reserved_amount = 0.0
    reserved_block_ids = []
    allowances_to_update = []

    for allowance in available_allowances:
        if reserved_amount >= amount:
            break

        # Link this allowance to the booking
        allowance.allowance_booking = booking
        allowances_to_update.append(allowance)

        reserved_amount += allowance.allowance
        reserved_block_ids.append(allowance.block.block_number)

    # Bulk update all allowances at once
    if allowances_to_update:
        BlockAllowance.objects.bulk_update(allowances_to_update, ['allowance_booking'])

    return booking.id, reserved_block_ids
    

def undo_allowance_reservation(reservation_id_: reservation_id) -> None:
    """
    Undo a previously made allowance reservation.

    This releases the reserved allowance back to the available pool.

    raises ReservationNotFound if the reservation is not found.
    """
    try:
        # Find the booking by reservation ID
        booking = AllowanceBooking.objects.get(id=reservation_id_)
    except AllowanceBooking.DoesNotExist:
        raise ReservationNotFound(f"Reservation with ID {reservation_id_} not found")

    # Find all BlockAllowances associated with this booking
    associated_allowances = BlockAllowance.objects.filter(allowance_booking=booking)

    # Release the allowances back to the available pool
    allowances_to_update = []
    for allowance in associated_allowances:
        allowance.allowance_booking = None
        allowances_to_update.append(allowance)

    # Bulk update all allowances
    if allowances_to_update:
        BlockAllowance.objects.bulk_update(allowances_to_update, ['allowance_booking'])

    # Delete the booking record
    booking.delete()

def spend_allowance(reservation_id_: reservation_id) -> None:
    """
    Spend allowance (make a reservation permanent).

     Args:
         reservation_id_: reservation_id

     raises ReservationNotFound if the reservation is not found.
    """
    updated_count = AllowanceBooking.objects.filter(
        id=reservation_id_
    ).update(
        is_spent=True,
        is_reserved=False,
        reservation_expiry_time=None
    )
    
    if updated_count == 0:
        raise ReservationNotFound(f"Reservation with ID {reservation_id_} not found")
