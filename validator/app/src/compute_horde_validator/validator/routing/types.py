class JobRoutingException(Exception):
    pass


class AllMinersBusy(JobRoutingException):
    pass
