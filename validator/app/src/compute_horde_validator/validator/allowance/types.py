import pydantic

ss58_address = str
reservation_id = int
block_id = int
block_ids = list[int]


class Miner(pydantic.BaseModel):
    address: str
    port: int
    hotkey_ss58: ss58_address


class Neuron(pydantic.BaseModel):
    hotkey_ss58: ss58_address
    coldkey: ss58_address


class AllowanceException(Exception):
    pass


class NeuronSnapshotMissing(AllowanceException):
    pass


class ReservationNotFound(AllowanceException):
    pass


class ReservationAlreadySpent(AllowanceException):
    pass


class CannotReserveAllowanceException(AllowanceException):
    """Exception raised when there is not enough allowance from a particular miner."""
    def __init__(self, miner: ss58_address, amount: float, total_available: float):
        self.miner = miner
        self.amount = amount
        self.total_available = total_available

    def __str__(self):
        return f"Not enough allowance from miner {self.miner}. Required: {self.amount}, Available: {self.total_available}"

    def to_dict(self) -> dict:
        """
        Convert exception attributes to dictionary for easier testing.

        Returns:
            Dictionary containing all exception attributes
        """
        return {
            'miner': self.miner,
            'amount': self.amount,
            'total_available': self.total_available,
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

    def to_dict(self) -> dict:
        """
        Convert exception attributes to dictionary for easier testing.

        Returns:
            Dictionary containing all exception attributes
        """
        return {
            'highest_available_allowance': self.highest_available_allowance,
            'highest_available_allowance_ss58': self.highest_available_allowance_ss58,
            'highest_unspent_allowance': self.highest_unspent_allowance,
            'highest_unspent_allowance_ss58': self.highest_unspent_allowance_ss58,
        }

    def __str__(self):
        return f"NotEnoughAllowanceException(highest_available_allowance={self.highest_available_allowance}, highest_available_allowance_ss58={self.highest_available_allowance_ss58}, highest_unspent_allowance={self.highest_unspent_allowance}, highest_unspent_allowance_ss58={self.highest_unspent_allowance_ss58})"
