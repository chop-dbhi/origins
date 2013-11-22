from __future__ import division, unicode_literals


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
