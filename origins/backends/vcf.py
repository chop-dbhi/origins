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

        index = 0
        fields = []

        # Fixed fields
        for name, desc in FIXED_FIELDS:
            fields.append({
                'name': name,
                'description': desc,
                'index': index,
            })
            index += 1

        keys = ('name', 'num_values', 'type', 'description')

        # INFO fields
        for row in r.infos.values():
            attrs = dict(zip(keys, row))
            attrs['index'] = index
            index += 1
            fields.append(attrs)

        # FORMAT fields
        for row in r.formats.values():
            attrs = dict(zip(keys, row))
            attrs['index'] = index
            index += 1
            fields.append(attrs)

        return fields


class File(base.Node):
    def sync(self):
        self.update(self.client.file())
        self.defines(self.client.fields(), Field)

    @property
    def fields(self):
        return self.definitions('field', sort='index')


class Field(base.Node):
    pass


# Export for API
Origin = File
