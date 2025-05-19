from pydantic import BaseModel


class StreamingDetails(BaseModel):
    """
    Streaming configuration details for a job.
    """
    public_key: str
    """
    The client public certificate.
    """
