from __future__ import division, print_function, unicode_literals, \
    absolute_import
from datetime import datetime
from urlparse import urlparse


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


def parse_uri(uri):
    p = urlparse(uri)
    scheme = p.scheme

    # Override to use an empty scheme since the database schemes are not
    # recognized and will not parse correctly. Set the scheme after the
    # default parsing occurs.
    if scheme:
        skip = '{}://'.format(scheme)
        p = urlparse(uri[len(skip):])

    return {
        'scheme': scheme,
        'host': p.hostname,
        'port': p.port,
        'username': p.username,
        'password': p.password,
        'path': p.path,
    }


def timestamp():
    "Returns a ISO 8601 UTC datetime string."
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')


def repr_lines(kw):
    "Returns a newline-delimited block of summary data."
    return '\n'.join('{}: {}'.format(k, kw[k]) for k in kw)


def res(instance, attrname, *args, **kwargs):
    """Returns of the result of an attribute/method on instance. If the
    attribute is callable, the arguments are passed in.
    """
    if hasattr(instance, attrname):
        attr = getattr(instance, attrname)
        if callable(attr):
            return attr(*args, **kwargs)
        return attr
