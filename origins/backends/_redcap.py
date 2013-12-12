from __future__ import division, absolute_import
from ..utils import cached_property
from . import base


class Project(base.Node):
    def sync(self):
        self.update(self.client.project())
        self._contains(self.client.forms(), Form)

    @property
    def forms(self):
        return self._containers('form')


class Form(base.Node):
    def sync(self):
        self._contains(self.client.fields(self['name']), Field)

    @property
    def fields(self):
        return self._containers('field')


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
