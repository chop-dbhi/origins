from __future__ import division, absolute_import
from ..utils import cached_property
from . import base

import redcap


class Client(base.Client):
    def __init__(self, name, url, token, **kwargs):
        self.project_name = name
        self.url = url
        self.token = token
        self.verify_ssl = kwargs.get('verify_ssl', True)

    @cached_property
    def project(self):
        return redcap.Project(self.url, self.token, name=self.project_name,
                              verify_ssl=self.verify_ssl)

    def forms(self):
        forms = []
        for name in self.project.forms:
            forms.append({'form_name': name})
        return forms

    def fields(self, form_name):
        fields = []

        for field in self.project.metadata:
            # Filter by form_name
            if field['form_name'] != form_name:
                continue

            identifier = field['identifier'].lower() == 'y' and True or False
            required = field['required_field'].lower() == 'y' and True or False

            fields.append({
                'field_name': field['field_name'],
                'field_label': field['field_label'],
                'field_note': field['field_note'],
                'field_type': field['field_type'],
                'field_choices': field['select_choices_or_calculations'],
                'field_validation_type': field['text_validation_type_or_show_slider_number'],
                'field_validation_min': field['text_validation_min'],
                'field_validation_max': field['text_validation_max'],
                'identifier': identifier,
                'required': required,
            })
        return fields


class Project(base.Node):
    label_attribute = 'project_name'
    branches_property = 'forms'

    def synchronize(self):
        self.attrs['project_name'] = self.client.project_name

    @cached_property
    def forms(self):
        nodes = []
        for attrs in self.client.forms():
            node = Form(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Form(base.Node):
    label_attribute = 'form_name'
    elements_property = 'fields'

    @cached_property
    def fields(self):
        nodes = []
        for attrs in self.client.fields(self['form_name']):
            node = Field(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Field(base.Node):
    label_attribute = 'field_name'

    @cached_property
    def choices(self):
        if self.attrs['field_choices']:
            choices = []
            lines = self.attrs['field_choices'].split(' \\n ')
            for line in lines:
                value, key = line.strip().split(', ', 1)
                choices.append((int(value), key))
            return choices

# Exported classes for API
Origin = Project
