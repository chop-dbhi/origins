from __future__ import unicode_literals, absolute_import

from collections import OrderedDict


def recordtuple(fields=None):
    """Creates a tuple class with support for field/key-based access.
    All `fields` must hashable
    """

    if not fields:
        fields = ()

    class record(tuple):
        "Lightweight tuple-based record with key-based access."
        __slots__ = ()

        _len = len(fields)
        _fields = tuple(fields)
        _fieldmap = {f: i for i, f in enumerate(fields)}

        def __new__(cls, *args):
            if len(args) != cls._len:
                raise TypeError('Expect {:d} arguments, got {:d}'
                                .format(cls._len, len(args)))
            return tuple.__new__(cls, args)

        def __repr__(self):
            fields = ', '.join(['{}={!r}'.format(self._fields[i], self[i])
                     for i in range(self._len)])
            return 'record({})'.format(fields)

        def __getitem__(self, i):
            if i in self._fieldmap:
                i = self._fieldmap[i]
            return tuple.__getitem__(self, i)

        @property
        def __dict__(self):
            return OrderedDict(zip(self._fields, self))

    return record
