from uvicorn.workers import UvicornWorker


class UvicornAsyncioWorker(UvicornWorker):
    CONFIG_KWARGS = {
        "loop": "asyncio",
    }
