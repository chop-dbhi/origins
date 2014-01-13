from __future__ import unicode_literals, absolute_import

from graphlib import Nodes
from ..utils import cached_property
from . import base


class Project(base.Node):
    def sync(self):
        self.update(self.client.project())
        self.defines(self.client.forms(), Form)

    @property
    def forms(self):
        return self.definitions('form')


class Form(base.Node):
    def sync(self):
        self.defines(self.client.sections(self['name']), Section)

    @property
    def sections(self):
        return self.definitions('section')

    @property
    def fields(self):
        fields = []
        for section in self.sections:
            fields.extend(section.fields)
        return Nodes(fields)


class Section(base.Node):
    def sync(self):
        form_name = self.parent['name']
        self.defines(self.client.fields(form_name, self['name']), Field)

    @property
    def fields(self):
        return self.definitions('field')


class Field(base.Node):
    @cached_property
    def choices(self):
        if self['choices']:
            choices = []
            lines = self['choices'].split(' \\n ')
            for line in lines:
                value, key = line.strip().split(', ', 1)
                choices.append((int(value), key))
            return choices
