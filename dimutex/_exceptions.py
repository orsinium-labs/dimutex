

class MutexError(Exception):
    pass


class AlreadyAcquiredError(MutexError):
    pass


class AlreadyReleasedError(MutexError):
    pass
