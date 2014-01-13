import origins
from .base import BackendTestCase

API_TOKEN = '54C661E0BAFFF731BBE3C642B8277E45'
API_URL = 'https://tiu.research.chop.edu/redcap/redcap/api/'


class RedcapApiClientTestCase(BackendTestCase):
    backend_path = 'origins.backends.redcap_api'

    def setUp(self):
        self.load_backend()
        self.client = self.backend.Client('redcap_demo', url=API_URL,
                                          token=API_TOKEN)

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
        fields = self.client.fields('demographics', 'default')
        self.assertEqual(len(fields), 3)
        self.assertTrue('name' in fields[0])


class RedcapApiApiTestCase(BackendTestCase):
    backend_path = 'origins.backends.redcap_api'

    def setUp(self):
        self.project = origins.connect('redcap-api', name='redcap_demo',
                                       url=API_URL, token=API_TOKEN)

    def test_project(self):
        self.assertEqual(self.project.label, 'redcap_demo')
        self.assertEqual(self.project.name, 'redcap_demo')
        self.assertEqual(self.project.path, 'redcap_demo')
        self.assertEqual(self.project.uri,
                         'redcap_api://tiu.research.chop.edu/redcap_demo')
        self.assertEqual(self.project.relpath, [])
        self.assertTrue(self.project.isroot)
        self.assertFalse(self.project.isleaf)
        self.assertTrue('uri' in self.project.serialize())

    def test_form(self):
        form = self.project.forms['demographics']

        self.assertEqual(form.label, 'demographics')
        self.assertEqual(form.name, 'demographics')
        self.assertEqual(form.path, 'redcap_demo/demographics')
        self.assertEqual(len(form.relpath), 1)
        self.assertFalse(form.isroot)
        self.assertFalse(form.isleaf)

        # Sections are not required in a form. This is direct access to the
        # fields.
        self.assertTrue(form.fields)

    def test_section(self):
        section = self.project.forms['demographics'].sections['default']

        self.assertEqual(section.label, 'Default')
        self.assertEqual(section.name, 'default')
        self.assertEqual(section.path, 'redcap_demo/demographics/default')
        self.assertEqual(len(section.relpath), 2)
        self.assertFalse(section.isroot)
        self.assertFalse(section.isleaf)

    def test_field(self):
        field = self.project.forms['demographics'].sections['default']\
            .fields['date_enrolled']

        self.assertEqual(field.label, 'Date subject signed consent')
        self.assertEqual(field.name, 'date_enrolled')
        self.assertEqual(field.path, 'redcap_demo/demographics/default/'
                                     'date_enrolled')
        self.assertEqual(len(field.relpath), 3)
        self.assertFalse(field.isroot)
        self.assertTrue(field.isleaf)

    def test_field_choices(self):
        field = self.project.forms['demographics'].fields['race']
        self.assertTrue(field.choices)
