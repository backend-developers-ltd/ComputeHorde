import abc


class ExecutorUnavailable(Exception):
    pass


class BaseExecutorManager(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def reserve_executor(self, token):
        """Start spinning up an executor with `token` or raise ExecutorUnavailable if at capacity"""

    @abc.abstractmethod
    async def destroy_executor(self, token):
        """Destroy a running executor with `token`"""
