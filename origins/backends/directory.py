from __future__ import division, unicode_literals
from . import base
from ..constants import DATETIME_FORMAT

import os
from datetime import datetime


class Directory(base.Node):
    elements_property = 'files'

    @property
    def label(self):
        return os.path.abspath(self['path'])

    def elements(self):
        nodes = []

        for root, dirs, files in os.walk(self.client.path):
            for f in files:
                path = os.path.join(root, f)
                attrs = {'path': os.path.relpath(path, self.client.path)}
                node = File(attrs=attrs, source=self, client=self.client)
                nodes.append(node)
        return nodes


class File(base.Node):
    label_attribute = 'path'

    def synchronize(self):
        stats = os.stat(self['path'])

        # Convert into datetime from timestamp floats
        atime = datetime.fromtimestamp(stats.st_atime)
        mtime = datetime.fromtimestamp(stats.st_mtime)
        ctime = datetime.fromtimestamp(stats.st_birthtime or stats.st_ctime)

        self.attrs.update({
            'mode': stats.st_mode,
            'uid': stats.st_uid,
            'gid': stats.st_gid,
            'size': stats.st_size,
            'accessed': atime.strftime(DATETIME_FORMAT),
            'modified': mtime.strftime(DATETIME_FORMAT),
            'created': ctime.strftime(DATETIME_FORMAT),
        })


# Exported classes for API
Origin = Directory


class Client(object):
    def __init__(self, path, **kwargs):
        self.path = os.path.abspath(path)
