from pydantic import BaseModel

class StreamingDetails(BaseModel):
    public_key: str
