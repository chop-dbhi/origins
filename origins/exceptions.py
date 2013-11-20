class OriginsError(Exception):
    pass


class UnknownBackend(OriginsError):
    def __init__(self, backend):
        message = 'unknown backend: {}'.format(backend)
        super(UnknownBackend, self).__init__(message)


class BackendNotSupported(OriginsError):
    def __init__(self, backend):
        message = 'backend not supported: {}'.format(backend)
        super(BackendNotSupported, self).__init__(message)
