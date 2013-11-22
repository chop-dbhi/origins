# unicode_literals not imported because it conflicts with the csv module
from __future__ import division, print_function, absolute_import
from . import base

import os
import csv


class File(base.Node):
    elements_property = 'columns'

    @property
    def label(self):
        return os.path.basename(self['path'])

    def elements(self):
        nodes = []
        for i, name in enumerate(self.client.header):
            attrs = {'name': name, 'position': i}
            node = Column(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes

    def synchronize(self):
        self.attrs['count'] = self.count()

    def count(self):
        n = 0
        for _ in self.client.reader:
            n += 1
        return n


class Column(base.Node):
    label_attribute = 'name'


# Exported classes for API
Origin = File


class Client(object):
    def __init__(self, path, header=None, delimiter=',', sniff=1024,
                 dialect=None, **kwargs):

        # Infer various attributes from the file including the dialect,
        # whether the sample
        with open(path, 'rb') as f:
            sniffer = csv.Sniffer()
            sample = '\n'.join(l for l in f.readlines(1024))
            f.seek(0)

            if not dialect:
                dialect = sniffer.sniff(sample)

            # Detect if a header is present to omit it from the count and
            # to use as the column names
            has_header = sniffer.has_header(sample)

            # Extract header names and fallback to positional values
            if not header:
                if has_header:
                    header = csv.DictReader(f, dialect=dialect,
                                            delimiter=delimiter).fieldnames
                else:
                    r = csv.reader(f, dialect=dialect, delimiter=delimiter)
                    header = range(len(next(r)))

            self.path = path
            self.header = header
            self.has_header = has_header
            self.dialect = dialect
            self.delimiter = delimiter

    @property
    def reader(self):
        f = open(self.path, 'rb')
        if self.has_header:
            next(f)
        return csv.reader(f, dialect=self.dialect, delimiter=self.delimiter)
