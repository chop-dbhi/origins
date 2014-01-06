from __future__ import unicode_literals, absolute_import

try:
    from urllib import quote as urlquote
except ImportError:
    from urllib.parse import quote as urlquote

PATH_SEPERATOR = '/'


class cached_property(object):
    """Decorator that converts a method with a single self argument into a
    property cached on the instance.
    """
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, type=None):
        if instance is None:
            return self
        result = self.func(instance)
        instance.__dict__[self.func.__name__] = result
        return result


def res(instance, attrname, *args, **kwargs):
    """Returns of the result of an attribute/method on instance. If the
    attribute is callable, the arguments are passed in.
    """
    if hasattr(instance, attrname):
        attr = getattr(instance, attrname)
        if callable(attr):
            return attr(*args, **kwargs)
        return attr


def build_uri(scheme, host=None, port=None, path=None):
    """Builds an Origins-based URI which acts as a unique identifier
    for external use.
    """
    if not scheme:
        raise ValueError('cannot have an empty scheme')

    host = host or ''

    if not path:
        path = PATH_SEPERATOR
    elif not path.startswith(PATH_SEPERATOR):
        path = PATH_SEPERATOR + path

    if port:
        netloc = '{}:{}'.format(host, port)
    else:
        netloc = host

    return '{}://{}{}'.format(scheme, netloc, urlquote(path))
