from __future__ import division, unicode_literals, absolute_import
from . import base

import os
import vcf


class File(base.Node):
    label_attribute = 'path'
    elements_property = 'fields'

    def elements(self):
        nodes = []

        r = self.client.reader

        # Fixed fields
        for field, desc in self.client.FIXED_FIELDS:
            attrs = {'field_name': field, 'desc': desc}
            node = Field(attrs=attrs, source=self, client=self.client)
            nodes.append(node)

        # INFO fields
        for field, info in r.infos.items():
            attrs = dict(zip(info._fields, info))
            del attrs['id']  # this will be set as the field_name
            attrs['field_name'] = field
            node = Field(attrs=attrs, source=self, client=self.client)
            nodes.append(node)

        # FORMAT fields
        for field, info in r.formats.items():
            attrs = dict(zip(info._fields, info))
            del attrs['id']  # this will be set as the field_name
            attrs['field_name'] = field
            node = Field(attrs=attrs, source=self, client=self.client)
            nodes.append(node)

        return nodes

    def synchronize(self):
        self.attrs.update(self.client.reader.metadata)


class Field(base.Node):
    label_attribute = 'field_name'


Origin = File


class Client(object):
    FIXED_FIELDS = (
        ('CHROM', 'Chromosome'),
        ('POS', 'Position'),
        ('ID', 'Unique identifier'),
        ('REF', 'Reference base(s)'),
        ('ALT', 'Alternate non-reference alleles'),
        ('QUAL', 'Phred-scale quality score for assertion made in ALT'),
        ('FILTER', 'PASS or filters that have not passed'),
    )

    def __init__(self, path, **kwargs):
        self.path = os.path.abspath(path)

    @property
    def _file(self):
        return open(self.path)

    @property
    def reader(self):
        return vcf.Reader(self._file)
