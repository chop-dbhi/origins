from __future__ import unicode_literals, absolute_import

import os
import origins
from .base import BackendTestCase, TEST_DATA_DIR

FILENAME = 'redcap_demo.csv'


class RedcapCsvClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.redcap_csv'

    def setUp(self):
        self.load_backend()
        path = os.path.join(TEST_DATA_DIR, FILENAME)
        self.client = self.backend.Client(path)

    def test_project(self):
        self.assertTrue(self.client.project())

    def test_forms(self):
        forms = self.client.forms()
        self.assertEqual(len(forms), 11)
        self.assertTrue('name' in forms[0])

    def test_sections(self):
        sections = self.client.sections('demographics')
        self.assertEqual(len(sections), 5)
        self.assertTrue('name' in sections[0])

    def test_fields(self):
        fields = self.client.fields('demographics', 'demographics')
        self.assertEqual(len(fields), 3)
        self.assertTrue('name' in fields[0])


class RedcapCsvApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.redcap_csv'

    def setUp(self):
        path = os.path.join(TEST_DATA_DIR, FILENAME)
        self.project = origins.connect('redcap-csv', path=path)

    def test_project(self):
        self.assertEqual(self.project.label, FILENAME)
        self.assertEqual(self.project.name, FILENAME)
        self.assertEqual(self.project.path, FILENAME)
        self.assertEqual(self.project.uri, 'redcap_csv:///' + FILENAME)
        self.assertEqual(self.project.relpath, [])
        self.assertTrue(self.project.isroot)
        self.assertFalse(self.project.isleaf)
        self.assertTrue('uri' in self.project.serialize())

    def test_form(self):
        form = self.project.forms['demographics']

        self.assertEqual(form.label, 'demographics')
        self.assertEqual(form.name, 'demographics')
        self.assertEqual(form.path, FILENAME + '/demographics')
        self.assertEqual(len(form.relpath), 1)
        self.assertFalse(form.isroot)
        self.assertFalse(form.isleaf)

        # Sections are not required in a form. This is direct access to the
        # fields.
        self.assertTrue(form.fields)

    def test_section(self):
        section = self.project.forms['demographics'].sections['demographics']

        self.assertEqual(section.label, 'demographics')
        self.assertEqual(section.name, 'demographics')
        self.assertEqual(section.path, FILENAME + '/demographics/demographics')
        self.assertEqual(len(section.relpath), 2)
        self.assertFalse(section.isroot)
        self.assertFalse(section.isleaf)

    def test_field(self):
        field = self.project.forms['demographics'].sections['demographics']\
            .fields['date_enrolled']

        self.assertEqual(field.label, 'Date subject signed consent')
        self.assertEqual(field.name, 'date_enrolled')
        self.assertEqual(field.path, FILENAME +
                         '/demographics/demographics/date_enrolled')
        self.assertEqual(len(field.relpath), 3)
        self.assertFalse(field.isroot)
        self.assertTrue(field.isleaf)

    def test_field_choices(self):
        field = self.project.forms['demographics'].fields['race']
        self.assertTrue(field.choices)
