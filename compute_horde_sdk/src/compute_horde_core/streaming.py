from pydantic import BaseModel


class StreamingDetails(BaseModel):
    public_key: str
    executor_ip: str | None = None  # set by miner before sending to executor
