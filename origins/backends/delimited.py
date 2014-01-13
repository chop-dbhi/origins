# unicode_literals not imported because it conflicts with the csv module
from __future__ import division, absolute_import
from . import base, _file

import csv


class Client(_file.Client):
    def __init__(self, path, header=None, delimiter=',', sniff=1024,
                 dialect=None, **kwargs):

        super(Client, self).__init__(path, **kwargs)

        # Infer various attributes from the file including the dialect,
        # whether the sample
        with open(self.file_path, 'rU') as f:
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

            self.header = header
            self.has_header = has_header
            self.dialect = dialect
            self.delimiter = delimiter

            # Map of column name to index
            self._header_index = dict(zip(header, range(len(header))))

    @property
    def file_handler(self):
        f = open(self.file_path, 'rU')
        if self.has_header:
            next(f)
        return f

    @property
    def reader(self):
        return csv.reader(self.file_handler, dialect=self.dialect,
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
        self.defines(self.client.columns(), Column)

    @property
    def columns(self):
        return self.definitions('column')


class Column(base.Node):
    pass


# Export for API
Origin = File
