from collections.abc import Mapping

import pydantic
from pydantic import ConfigDict

ss58_address = str
reservation_id = int
block_id = int
block_ids = list[int]


class MetagraphData(pydantic.BaseModel):
    block: int
    block_hash: str
    neurons: list["Neuron"]
    subnet_state: Mapping[str, list[float]]
    alpha_stake: list[float]
    tao_stake: list[float]
    total_stake: list[float]
    uids: list[int]
    hotkeys: list[str]
    coldkeys: list[str | None]
    serving_hotkeys: list[str]

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def block_number(self) -> int:
        return self.block


class Miner(pydantic.BaseModel):
    address: str
    ip_version: int
    port: int
    hotkey_ss58: ss58_address


class Neuron(pydantic.BaseModel):
    hotkey: ss58_address
    coldkey: ss58_address | None


class ValidatorModel(pydantic.BaseModel):
    uid: int
    hotkey: ss58_address
    effective_stake: float


class SpendingDetails(pydantic.BaseModel):
    requested_amount: float
    offered_blocks: block_ids
    spendable_amount: float
    spent_blocks: block_ids
    outside_range_blocks: block_ids
    double_spent_blocks: block_ids
    invalidated_blocks: block_ids

    @property
    def rejected_blocks(self) -> block_ids:
        return self.outside_range_blocks + self.double_spent_blocks + self.invalidated_blocks


class AllowanceException(Exception):
    pass


class NeuronSnapshotMissing(AllowanceException):
    pass


class ReservationNotFound(AllowanceException):
    pass


class ReservationAlreadySpent(AllowanceException):
    pass


class CannotSpend(Exception):
    def __init__(self, details: SpendingDetails):
        self.details = details

    def __str__(self):
        return f"Cannot spend ({self.details})"


class ErrorWhileSpending(Exception):
    pass


class CannotReserveAllowanceException(AllowanceException):
    """Exception raised when there is not enough allowance from a particular miner."""

    def __init__(
        self,
        miner: ss58_address,
        required_allowance_seconds: float,
        available_allowance_seconds: float,
    ):
        self.miner = miner
        self.required_allowance_seconds = required_allowance_seconds
        self.available_allowance_seconds = available_allowance_seconds

    def __str__(self):
        return f"Not enough allowance from miner {self.miner}. Required: {self.required_allowance_seconds}, Available: {self.available_allowance_seconds}"

    def to_dict(self) -> dict[str, ss58_address | float]:
        """
        Convert exception attributes to dictionary for easier testing.

        Returns:
            Dictionary containing all exception attributes
        """
        return {
            "miner": self.miner,
            "required_allowance_seconds": self.required_allowance_seconds,
            "available_allowance_seconds": self.available_allowance_seconds,
        }


class NotEnoughAllowanceException(AllowanceException):
    """Exception raised when there is not enough allowance."""

    def __init__(
        self,
        highest_available_allowance: float,
        highest_available_allowance_ss58: ss58_address,
        highest_unspent_allowance: float,
        highest_unspent_allowance_ss58: ss58_address,
    ):
        """
        :param highest_available_allowance: highest number of executor-seconds available
        :param highest_available_allowance_ss58: hotkey of the miner with highest number of executor-seconds available
        :param highest_unspent_allowance: highest number of executor-seconds unspent (free or reserved)
        :param highest_unspent_allowance_ss58: hotkey of the miner with highest number of executor-seconds unspent
        """
        self.highest_available_allowance = highest_available_allowance
        self.highest_available_allowance_ss58 = highest_available_allowance_ss58
        self.highest_unspent_allowance = highest_unspent_allowance
        self.highest_unspent_allowance_ss58 = highest_unspent_allowance_ss58

    def to_dict(self) -> dict[str, ss58_address | float]:
        """
        Convert exception attributes to dictionary for easier testing.

        Returns:
            Dictionary containing all exception attributes
        """
        return {
            "highest_available_allowance": self.highest_available_allowance,
            "highest_available_allowance_ss58": self.highest_available_allowance_ss58,
            "highest_unspent_allowance": self.highest_unspent_allowance,
            "highest_unspent_allowance_ss58": self.highest_unspent_allowance_ss58,
        }

    def __str__(self):
        return f"NotEnoughAllowanceException(highest_available_allowance={self.highest_available_allowance}, highest_available_allowance_ss58={self.highest_available_allowance_ss58}, highest_unspent_allowance={self.highest_unspent_allowance}, highest_unspent_allowance_ss58={self.highest_unspent_allowance_ss58})"
