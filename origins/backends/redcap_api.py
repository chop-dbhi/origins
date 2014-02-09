from __future__ import division, absolute_import

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
from ..utils import cached_property
from . import base, _redcap

import redcap


class Client(base.Client):
    def __init__(self, name, url, token, **kwargs):
        self.project_name = name
        self.url = url
        self.token = token
        self.verify_ssl = kwargs.get('verify_ssl', True)
        # Parsed for constructing the URI
        self.host = urlparse(url).hostname

    @cached_property
    def _project(self):
        return redcap.Project(self.url, self.token, name=self.project_name,
                              verify_ssl=self.verify_ssl)

    def project(self):
        return {
            'name': self.project_name,
            'url': self.url,
        }

    def forms(self):
        order = 0
        forms = []
        unique = set()

        for field in self._project.metadata:
            name = field['form_name']

            if name not in unique:
                forms.append({'name': name, 'order': order})
                order += 1
                unique.add(name)

        return forms

    def sections(self, form_name):
        sections = [{
            'name': 'default',
            'label': 'Default',
            'order': 0
        }]
        order = 1
        unique = set()

        for field in self._project.metadata:
            # Filter by form_name
            if field['form_name'] != form_name or not field['section_header']:
                continue

            name = field['section_header']

            if name not in unique:
                sections.append({'name': name, 'order': order})
                order += 1
                unique.add(name)

        return sections

    def fields(self, form_name, section_name):
        order = 0
        fields = []
        current_section = 'default'

        for field in self._project.metadata:
            # Filter by form_name
            if field['form_name'] != form_name:
                order += 1
                continue

            # Filter by section_name
            current_section = field['section_header'] or current_section
            if current_section != section_name:
                order += 1
                continue

            identifier = field['identifier'].lower() == 'y' and True or False
            required = field['required_field'].lower() == 'y' and True or False

            fields.append({
                'name': field['field_name'],
                'label': field['field_label'],
                'note': field['field_note'],
                'type': field['field_type'],
                'choices': field['select_choices_or_calculations'],
                'display_logic': field['branching_logic'],
                'validation_type': field['text_validation_type_or_show_slider_number'],  # noqa
                'validation_min': field['text_validation_min'],
                'validation_max': field['text_validation_max'],
                'identifier': identifier,
                'required': required,
                'alignment': field['custom_alignment'],
                'survey_num': field['question_number'],
                'matrix': field['matrix_group_name'],
                'order': order,
            })

            order += 1

        return fields


# Export class for API
Origin = _redcap.Project
