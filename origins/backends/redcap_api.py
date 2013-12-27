from __future__ import division, absolute_import
from ..utils import cached_property
from . import base, _redcap

import redcap


class Client(base.Client):
    def __init__(self, name, url, token, **kwargs):
        self.project_name = name
        self.url = url
        self.token = token
        self.verify_ssl = kwargs.get('verify_ssl', True)

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
        forms = []
        unique = set()
        for field in self._project.metadata:
            name = field['form_name']
            if name not in unique:
                forms.append({'name': name})
                unique.add(name)
        return forms

    def fields(self, form_name):
        fields = []

        for field in self._project.metadata:
            # Filter by form_name
            if field['form_name'] != form_name:
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
                'header': field['section_header'],
                'alignment': field['custom_alignment'],
                'survey_num': field['question_number'],
                'matrix': field['matrix_group_name'],
            })
        return fields


# Export class for API
Origin = _redcap.Project
