from __future__ import division, unicode_literals, absolute_import
from ..utils import cached_property
from . import base, _file

import vcf


FIXED_FIELDS = (
    ('CHROM', 'Chromosome'),
    ('POS', 'Position'),
    ('ID', 'Unique identifier'),
    ('REF', 'Reference base(s)'),
    ('ALT', 'Alternate non-reference alleles'),
    ('QUAL', 'Phred-scale quality score for assertion made in ALT'),
    ('FILTER', 'PASS or filters that have not passed'),
)


class Client(_file.Client):
    @property
    def reader(self):
        return vcf.Reader(self.file_handler)

    def fields(self):
        r = self.reader

        keys = ('field_name', 'possible_values', 'data_type', 'description')
        fields = []

        # Fixed fields
        for name, desc in FIXED_FIELDS:
            attrs = dict(zip(keys, (name, desc, None)))
            fields.append(attrs)

        # INFO fields
        for info in r.infos.values():
            attrs = dict(zip(keys, info[:3]))
            fields.append(attrs)

        # FORMAT fields
        for info in r.formats.values():
            attrs = dict(zip(keys, info[:3]))
            fields.append(attrs)

        return fields


class File(base.Node):
    elements_property = 'fields'

    def synchronize(self):
        super(File, self).synchronize()
        self.attrs.update(self.client.reader.metadata)

    @cached_property
    def fields(self):
        nodes = []
        for attrs in self.client.fields():
            node = Field(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Field(base.Node):
    label_attribute = 'field_name'


Origin = File
