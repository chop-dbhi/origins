from __future__ import division, print_function, unicode_literals, \
    absolute_import
from . import base

import os
import openpyxl


class Workbook(base.Node):
    label_attribute = 'path'
    branches_property = 'sheets'

    def branches(self):
        nodes = []
        for i, name in enumerate(self.client.workbook.get_sheet_names()):
            attrs = {'name': name, 'index': i}
            node = Sheet(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes


class Sheet(base.Node):
    elements_property = 'columns'

    def elements(self):
        workbook = self.client.workbook
        sheet = workbook.get_sheet_by_name(self['name'])

        if self.client.has_headers:
            header = [c.internal_value for c in next(sheet.iter_rows())]
        else:
            header = self.client.headers.get(self['name'])
            if not header:
                header = range(len(next(sheet.iter_rows())))

        nodes = []

        for i, column in enumerate(header):
            attrs = {'column': column, 'index': i}
            node = Column(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes

    def synchronize(self):
        sheet = self.client.workbook.get_sheet_by_name(self['name'])
        self.attrs['title'] = sheet.title


class Column(base.Node):
    label_attribute = 'column'


# Export to public API
Origin = Workbook


class Client(object):
    def __init__(self, path, headers=True, **kwargs):
        self.path = os.path.join(path)

        if isinstance(headers, bool):
            self.has_headers = headers
            self.headers = {}
        else:
            self.has_headers = False
            first_sheet = self.workbook.get_sheet_names()[0]
            if isinstance(headers, (list, tuple)):
                self.headers = {first_sheet: headers}
            else:
                self.headers = headers or {}

    @property
    def workbook(self):
        return openpyxl.load_workbook(self.path, use_iterators=True)
