# unicode_literals not imported because it conflicts with the csv module
from __future__ import division, absolute_import

import csv
import logging
from .._csv import UnicodeCsvReader
from . import base, _file


logger = logging.getLogger(__name__)


class Client(_file.Client):
    def __init__(self, path, header=None, delimiter=',', sniff=1024,
                 dialect=None, **kwargs):

        super(Client, self).__init__(path, **kwargs)

        # Infer various attributes from the file including the dialect,
        # whether the sample
        with open(self.file_path, 'rU') as f:
            sample = '\n'.join([l for l in f.readlines(sniff)])
            f.seek(0)

            sniffer = csv.Sniffer()

            if not dialect:
                dialect = sniffer.sniff(sample)

            r = UnicodeCsvReader(f, dialect=dialect,
                                 delimiter=delimiter)

            # Get the first row as the header
            _header = r.next()

            # Explicit declaration of the header being present
            if header is True:
                has_header = True
                header = _header
            elif sniffer.has_header(sample):
                has_header = True
            else:
                has_header = False
                _header = range(len(_header))

            if not header:
                header = tuple(_header)

        self.header = header
        self.has_header = has_header
        self.dialect = dialect
        self.delimiter = delimiter

    @property
    def file_handler(self):
        f = open(self.file_path, 'rU')

        if self.has_header:
            next(f)

        return f

    @property
    def reader(self):
        return UnicodeCsvReader(self.file_handler, dialect=self.dialect,
                                delimiter=self.delimiter)

    def file(self):
        return {
            'path': self.file_path,
            'name': self.file_name,
            'delimiter': self.delimiter,
        }

    def columns(self):
        columns = []

        for i, name in enumerate(self.header):
            columns.append({
                'name': name,
                'index': i
            })

        return columns

    def file_line_count(self):
        n = 0

        for _ in self.file_handler:
            n += 1

        return n


class File(_file.File):
    def sync(self):
        super(File, self).sync()
        self.define(self.client.columns(), Column)

    @property
    def columns(self):
        return self.definitions('column', sort='index')


class Column(base.Node):
    pass


# Export for API
Origin = File
