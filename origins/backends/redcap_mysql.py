from __future__ import division, unicode_literals, absolute_import

from . import mysql, _redcap


class Client(mysql.Client):
    def __init__(self, project, database='redcap', **kwargs):
        self.project_name = project
        super(Client, self).__init__(database, **kwargs)

    def projects(self):
        query = '''
            SELECT
                project_name,
                app_title
            FROM redcap_projects
        '''

        keys = ('name', 'label')

        projects = []

        for row in self.fetchall(query):
            attrs = dict(zip(keys, row))
            if not attrs['label']:
                attrs['label'] = attrs['name']
            projects.append(attrs)

        return projects

    def project(self):
        query = '''
            SELECT
                project_name,
                app_title
            FROM redcap_projects
            WHERE project_name = %s
        '''

        keys = ('name', 'label')
        values = self.fetchone(query, [self.project_name])
        return dict(zip(keys, values))

    def forms(self):
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

        keys = ('name', 'label')

        forms = []

        for i, row in enumerate(self.fetchall(query, [self.project_name])):
            attrs = dict(zip(keys, row))
            if not attrs['label']:
                attrs['label'] = attrs['name']
            attrs['order'] = i
            forms.append(attrs)

        return forms

    def sections(self, form_name):
        query = '''
            SELECT DISTINCT
                element_preceding_header
            FROM redcap_metadata JOIN redcap_projects
                ON (redcap_metadata.project_id = redcap_projects.project_id)
            WHERE redcap_projects.project_name = %s
                AND form_name = %s
                AND element_preceding_header IS NOT NULL
            ORDER BY field_order
        '''

        keys = ('name',)

        sections = [{
            'name': form_name,
            'order': 0,
        }]

        rows = self.fetchall(query, [self.project_name, form_name])

        for i, row in enumerate(rows):
            attrs = dict(zip(keys, row))
            attrs['order'] = i + 1
            sections.append(attrs)

        return sections

    def fields(self, form_name, section_name):
        query = '''
            SELECT
                field_name,
                element_label,
                element_type,
                element_note,
                element_enum,
                branching_logic,
                element_validation_type,
                element_validation_min,
                element_validation_max,
                field_phi,
                field_req,
                element_preceding_header,
                custom_alignment,
                question_num,
                grid_name,
                field_order
            FROM redcap_metadata JOIN redcap_projects
                ON (redcap_metadata.project_id = redcap_projects.project_id)
            WHERE redcap_projects.project_name = %s
                AND form_name = %s
            ORDER BY field_order
        '''

        keys = ('name', 'label', 'type', 'note', 'choices', 'display_logic',
                'validation_type', 'validation_min', 'validation_max',
                'identifier', 'required', 'header', 'alignment', 'survey_num',
                'matrix', 'order')

        fields = []
        current_section = form_name

        for row in self.fetchall(query, [self.project_name, form_name]):
            attrs = dict(zip(keys, row))
            attrs['required'] = bool(attrs['required'])
            attrs['identifier'] = bool(attrs['identifier'])

            # Filter by section_name
            current_section = attrs['header'] or current_section
            if current_section != section_name:
                continue

            # Remove header attribute since it is redundant with respect to
            # the section.
            attrs.pop('header')
            fields.append(attrs)

        return fields


# Export class for API
Origin = _redcap.Project
