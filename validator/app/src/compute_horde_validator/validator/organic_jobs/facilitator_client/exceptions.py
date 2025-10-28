class TransportLayerReceiveError(Exception):
    def __init__(self, cause: Exception) -> None:
        super().__init__(
            f"Error receiving message from transport layer: {type(cause).__name__}: {cause}"
        )


class LocalChannelReceiveError(Exception):
    def __init__(self, cause: Exception, channel: str) -> None:
        super().__init__(
            f"Error receiving message from local Django channel '{channel}': {type(cause).__name__}: {cause}"
        )


class LocalChannelSendError(Exception):
    def __init__(self, cause: Exception, channel: str) -> None:
        super().__init__(
            f"Error sending message to local Django channel '{channel}': {type(cause).__name__}: {cause}"
        )
