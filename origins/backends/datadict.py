from __future__ import division, absolute_import

from . import _file, base, delimited


class Client(delimited.Client):
    def fields(self):
        fields = []

        for i, row in enumerate(self.reader):
            field = dict(zip(self.header, row))
            field['index'] = i
            fields.append(field)

        return fields


class File(_file.File):
    @property
    def name_attribute(self):
        return self.client.header[0]

    def sync(self):
        super(File, self).sync()
        self.define(self.client.fields(), Field)

    @property
    def fields(self):
        return self.definitions('field', sort='index')


class Field(base.Component):
    pass


# Export for API
Origin = File
