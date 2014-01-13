from __future__ import division, unicode_literals, absolute_import
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

    def file(self):
        attrs = {
            'path': self.file_path,
            'name': self.file_name,
        }
        attrs.update(self.reader.metadata)
        return attrs

    def fields(self):
        r = self.reader

        keys = ('name', 'num_values', 'type', 'description')
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
    def sync(self):
        self.update(self.client.file())
        self.defines(self.client.fields(), Field)

    @property
    def fields(self):
        return self.definitions('field')


class Field(base.Node):
    pass


# Export for API
Origin = File
