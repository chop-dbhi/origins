from __future__ import unicode_literals, absolute_import

import os
import origins
import unittest
from .base import BackendTestCase

HOST = os.environ.get('REDCAP_MYSQL_HOST')
PORT = os.environ.get('REDCAP_MYSQL_PORT', 3306)
USER = os.environ.get('REDCAP_MYSQL_USER')
PASSWORD = os.environ.get('REDCAP_MYSQL_PASSWORD')


@unittest.skipUnless(HOST, 'connection settings not available')
class RedcapMysqlClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.redcap_mysql'

    def setUp(self):
        self.load_backend()
        self.client = self.backend.Client(host=HOST, port=PORT,
                                          user=USER, password=PASSWORD,
                                          project='redcap_demo')

    def test_project(self):
        self.assertTrue(self.client.project())

    def test_forms(self):
        forms = self.client.forms()
        self.assertTrue(forms)
        self.assertTrue('name' in forms[0])

    def test_sections(self):
        sections = self.client.sections('demographics')
        self.assertTrue(sections)
        self.assertTrue('name' in sections[0])

    def test_fields(self):
        fields = self.client.fields('demographics', 'demographics')
        self.assertTrue(fields)
        self.assertTrue('name' in fields[0])


@unittest.skipUnless(HOST, 'connection settings not available')
class RedcapMysqlApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.redcap_mysql'

    def setUp(self):
        self.project = origins.connect('redcap-mysql', host=HOST, port=PORT,
                                       user=USER, password=PASSWORD,
                                       project='redcap_demo')

    def test_project(self):
        self.assertEqual(self.project.label, 'REDCap Demo Database')
        self.assertEqual(self.project.name, 'redcap_demo')
        self.assertEqual(self.project.path, 'redcap_demo')
        self.assertEqual(self.project.uri, 'redcap_mysql://' + HOST + ':'
                                           + str(PORT) + '/redcap_demo')
        self.assertEqual(self.project.relpath, [])
        self.assertTrue(self.project.isroot)
        self.assertFalse(self.project.isleaf)
        self.assertTrue('uri' in self.project.serialize())

    def test_form(self):
        form = self.project.forms['demographics']

        self.assertEqual(form.label, 'Demographics')
        self.assertEqual(form.name, 'demographics')
        self.assertEqual(form.path, 'redcap_demo/demographics')
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
        self.assertEqual(section.path, 'redcap_demo/demographics/demographics')
        self.assertEqual(len(section.relpath), 2)
        self.assertFalse(section.isroot)
        self.assertFalse(section.isleaf)

    def test_field(self):
        field = self.project.forms['demographics'].sections['demographics']\
            .fields['date_enrolled']

        self.assertEqual(field.label, 'Date subject signed consent')
        self.assertEqual(field.name, 'date_enrolled')
        self.assertEqual(field.path, 'redcap_demo/demographics/demographics/'
                                     'date_enrolled')
        self.assertEqual(len(field.relpath), 3)
        self.assertFalse(field.isroot)
        self.assertTrue(field.isleaf)

    def test_field_choices(self):
        field = self.project.forms['demographics'].fields['race']
        self.assertTrue(field.choices)
