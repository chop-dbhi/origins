class OriginsError(Exception):
    pass


class DoesNotExist(OriginsError):
    pass


class ValidationError(OriginsError):
    pass


class InvalidState(OriginsError):
    pass
