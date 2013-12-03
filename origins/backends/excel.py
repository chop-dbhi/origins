from __future__ import division, unicode_literals, absolute_import
from ..utils import cached_property
from . import base, _file

import openpyxl


class Client(_file.Client):
    def __init__(self, path, headers=True, **kwargs):
        super(Client, self).__init__(path, **kwargs)

        self._sheet_columns = {}

        if isinstance(headers, bool):
            self.has_headers = headers
        else:
            self.has_headers = False
            first_sheet = self.workbook.get_sheet_names()[0]
            if isinstance(headers, (list, tuple)):
                self._sheet_columns[first_sheet] = headers
            elif headers:
                self._sheet_columns = headers

    @property
    def workbook(self):
        return openpyxl.load_workbook(self.file_path, use_iterators=True)

    def sheets(self):
        return [{
            'sheet_name': name,
            'sheet_index': i,
        } for i, name in enumerate(self.workbook.get_sheet_names())]

    def columns(self, sheet_name):
        "Return the sheet header names or indices."
        if sheet_name in self._sheet_columns:
            column_names = self._sheet_columns[sheet_name]
        else:
            sheet = self.workbook.get_sheet_by_name(sheet_name)
            first_row = next(sheet.iter_rows())
            if self.has_headers:
                column_names = [c.internal_value for c in first_row]
            else:
                column_names = range(len(first_row))

        columns = []
        for i, name in enumerate(column_names):
            columns.append({
                'column_name': name,
                'column_index': i
            })
        return columns


class Workbook(_file.Node):
    branches_property = 'sheets'

    @cached_property
    def sheets(self):
        nodes = []
        for attrs in self.client.sheets():
            node = Sheet(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Sheet(base.Node):
    label_attribute = 'sheet_name'
    elements_property = 'columns'

    @cached_property
    def columns(self):
        nodes = []
        for attrs in self.client.columns(self['sheet_name']):
            node = Column(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Column(base.Node):
    label_attribute = 'column_name'


# Export for API
Origin = Workbook
