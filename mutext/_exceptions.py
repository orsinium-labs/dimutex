

class MutexError(Exception):
    pass


class AlreadyAcquiredError(MutexError):
    pass
