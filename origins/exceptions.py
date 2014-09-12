class OriginsError(Exception):
    pass


class DoesNotExist(OriginsError):
    pass


class ValidationError(OriginsError):
    pass


class InvalidStateError(OriginsError):
    amendment = 'This error should not happen, please open an issue ' \
                'at https://github.com/cbmi/origins/issues'

    def __init__(self, message, *args, **kwargs):
        if not message.endswith('.'):
            message += '.'

        message = message + '\n' + self.amendment
        super(InvalidStateError, self).__init__(message, *args, **kwargs)
