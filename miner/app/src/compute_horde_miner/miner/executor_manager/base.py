import abc


class ExecutorUnavailable(Exception):
    pass


class BaseExecutorManager(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def reserve_executor(self, token):
        """Start spinning up an executor with `token` or raise ExecutorUnavailable if at capacity"""
