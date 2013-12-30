import os
import origins
from .base import BackendTestCase, TEST_DATA_DIR


class VcfClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.vcf'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, 'sample.vcf')
        self.client = self.backend.Client(path)

    def test_reader(self):
        row = next(self.client.reader)
        self.assertTrue(row.CHROM)

    def test_file(self):
        self.assertTrue(self.client.file())

    def test_fields(self):
        fields = self.client.fields()
        self.assertEqual(len(fields), 32)
        self.assertTrue('name' in fields[0])


class VcfApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.vcf'

    def setUp(self):
        self.path = os.path.join(TEST_DATA_DIR, 'sample.vcf')
        self.f = origins.connect('vcf', path=self.path)

    def test_file(self):
        self.assertEqual(self.f.label, 'sample.vcf')
        self.assertEqual(self.f.name, 'sample.vcf')
        self.assertEqual(self.f.path, 'sample.vcf')
        self.assertEqual(self.f.uri, 'vcf:///sample.vcf')
        self.assertEqual(self.f.relpath, [])
        self.assertTrue(self.f.isroot)
        self.assertFalse(self.f.isleaf)
        self.assertTrue('uri' in self.f.serialize())

    def test_field(self):
        field = self.f.fields['CHROM']

        self.assertEqual(field.label, 'CHROM')
        self.assertEqual(field.name, 'CHROM')
        self.assertEqual(field.path, 'sample.vcf/CHROM')
        self.assertEqual(len(field.relpath), 1)
        self.assertFalse(field.isroot)
        self.assertTrue(field.isleaf)
