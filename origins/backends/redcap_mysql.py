from __future__ import division, unicode_literals, absolute_import
from ..utils import cached_property
from . import base, mysql


class Client(mysql.Client):
    def __init__(self, project, database='redcap', **kwargs):
        self.project = project
        super(Client, self).__init__(database, **kwargs)

    def projects(self):
        query = '''
            SELECT
                project_id,
                project_name,
                app_title
            FROM redcap_projects
        '''

        keys = ('project_id', 'project_name', 'project_label')

        projects = []
        for row in self.fetchall(query):
            attrs = dict(zip(keys, row))
            if not attrs['project_label']:
                attrs['project_label'] = attrs['project_name']
            projects.append(attrs)
        return projects

    def forms(self, project_name):
        query = '''
            SELECT DISTINCT
                form_name,
                form_menu_description
            FROM redcap_metadata JOIN redcap_projects
                ON (redcap_metadata.project_id = redcap_projects.project_id)
            WHERE redcap_projects.project_name = %s
                AND form_menu_description IS NOT NULL
            ORDER BY field_order
        '''

        keys = ('form_name', 'form_label')

        forms = []
        for row in self.fetchall(query, [project_name]):
            attrs = dict(zip(keys, row))
            if not attrs['form_label']:
                attrs['form_label'] = attrs['form_name']
            forms.append(attrs)
        return forms

    def fields(self, project_name, form_name):
        query = '''
            SELECT
                field_name,
                element_label,
                field_order,
                element_type,
                element_note,
                element_enum,
                field_units,
                element_validation_type,
                element_validation_min,
                element_validation_max,
                field_req,
                field_phi
            FROM redcap_metadata JOIN redcap_projects
                ON (redcap_metadata.project_id = redcap_projects.project_id)
            WHERE redcap_projects.project_name = %s
                AND form_name = %s
            ORDER BY field_order
        '''

        keys = ('field_name', 'field_label', 'field_order', 'field_type',
                'field_note', 'field_choices', 'field_units',
                'field_validation_type', 'field_validation_min',
                'field_validation_max', 'required', 'identifier')

        fields = []
        for row in self.fetchall(query, [project_name, form_name]):
            attrs = dict(zip(keys, row))
            attrs['required'] = bool(attrs['required'])
            attrs['identifier'] = bool(attrs['identifier'])
            fields.append(attrs)
        return fields


class Project(base.Node):
    label_attribute = 'project_label'
    path_attribute = 'project_name'
    branches_property = 'forms'

    def synchronize(self):
        for attrs in self.client.projects():
            if attrs['project_name'] == self.client.project:
                self.attrs.update(attrs)
                return

    @cached_property
    def forms(self):
        nodes = []
        for attrs in self.client.forms(self['project_name']):
            node = Form(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Form(base.Node):
    label_attribute = 'form_label'
    path_attribute = 'form_name'
    elements_property = 'fields'

    @cached_property
    def fields(self):
        nodes = []
        for attrs in self.client.fields(self.source['project_name'],
                                        self['form_name']):
            node = Field(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Field(base.Node):
    label_attribute = 'field_label'
    path_attribute = 'field_name'

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
