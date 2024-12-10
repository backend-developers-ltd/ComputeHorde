from compute_horde.receipts.schemas import Receipt, ReceiptType

default_app_config = "compute_horde.receipts.apps.ComputeHordeReceiptsConfig"

# Reexported for compatibility. These were moved to submodules.
__all__ = [
    "Receipt",
    "ReceiptType",
]
