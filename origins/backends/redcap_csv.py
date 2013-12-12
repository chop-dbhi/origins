from __future__ import division, absolute_import
from ..utils import cached_property
from . import base, _file

import csv

METADATA_HEADER = ('field_name', 'form_name', 'section_header', 'field_type',
                   'field_label', 'choices', 'field_note',
                   'text_validation_type', 'text_validation_min',
                   'text_validation_max', 'identifier',
                   'branching_logic', 'required', 'custom_alignment',
                   'question_number', 'matrix_group_name')


def parse_filename(filename):
    return filename.split('.')[0].split('_')[0]


class Client(_file.Client):
    @property
    def file_handler(self):
        f = open(self.file_path, 'rU')
        # Skip the header
        next(f)
        return f

    @property
    def reader(self):
        return csv.DictReader(self.file_handler, fieldnames=METADATA_HEADER)

    def forms(self):
        "Collects all unique form names while maintaining the order."
        reader = self.reader
        forms = []
        unique = set()

        for row in reader:
            form_name = row['form_name']
            if form_name not in unique:
                forms.append({
                    'form_name': form_name
                })
        return forms

    def fields(self, form_name):
        reader = self.reader
        fields = []

        for row in reader:
            # Filter by form_name
            if row['form_name'] != form_name:
                continue

            identifier = row['identifier'].lower() == 'y' and True or False
            required = row['required'].lower() == 'y' and True or False

            fields.append({
                'field_name': row['field_name'],
                'field_type': row['field_type'],
                'field_label': row['field_label'],
                'field_choices': row['choices'],
                'field_note': row['field_note'],
                'field_validation_type': row['text_validation_type'],
                'field_validation_min': row['text_validation_min'],
                'field_validation_max': row['text_validation_max'],
                'identifier': identifier,
                'required': required,
            })
        return fields


class Project(_file.Node):
    label_attribute = 'project_name'
    branches_property = 'forms'

    def synchronize(self):
        super(Project, self).synchronize()
        self.attrs['project_name'] = parse_filename(self.client.file_name)

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
