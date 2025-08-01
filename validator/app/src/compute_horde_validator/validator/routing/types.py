class JobRoutingException(Exception):
    pass


class NoMinerForExecutorType(JobRoutingException):
    pass


class AllMinersBusy(JobRoutingException):
    pass


class MinerIsBlacklisted(JobRoutingException):
    pass


class NotEnoughTimeInCycle(JobRoutingException):
    pass


class NoMinerWithEnoughAllowance(JobRoutingException):
    pass
