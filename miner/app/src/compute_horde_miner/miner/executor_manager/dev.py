from compute_horde_miner.miner.executor_manager.base import BaseExecutorManager, ExecutorUnavailable


class DevExecutorManager(BaseExecutorManager):
    def reserve_executor(self, token):
        raise ExecutorUnavailable("Executor is not available for reservation")
