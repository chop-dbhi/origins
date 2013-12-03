from __future__ import division, unicode_literals
from ..constants import DATETIME_FORMAT
from ..utils import cached_property
from . import base, _file

import os
from datetime import datetime


class Client(base.Client):
    def __init__(self, path, **kwargs):
        self.directory_path = os.path.abspath(path)
        self.recurse = kwargs.get('recurse', True)

        if not self.recurse:
            self.depth = 0
        else:
            self.depth = kwargs.get('depth')

        # TODO support regexp or glob syntax for matching a subset of files
        # self.regexp = kwargs.get('regexp')

    def files(self):
        _files = []

        for root, dirs, files in os.walk(self.directory_path):
            if self.depth is not None:
                curpath = os.path.relpath(root, self.directory_path)

                if curpath != '.':
                    depth = 0
                else:
                    depth = len(curpath.split(os.path.sep))

                # Remove all subdirectories from traversal
                if depth >= self.depth:
                    [dirs.pop() for _ in xrange(len(dirs))]

            for f in files:
                path = os.path.join(root, f)
                _files.append({
                    'file_path': path,
                    'file_name': os.path.relpath(path, self.directory_path),
                })

        return _files


class Directory(base.Node):
    label_attribute = 'directory_path'
    elements_property = 'files'

    def synchronize(self):
        self['directory_path'] = self.client.directory_path
        self['directory_name'] = os.path.basename(self['directory_path'])

    @cached_property
    def files(self):
        nodes = []
        for attrs in self.client.files():
            node = File(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class File(_file.Node):
    def synchronize(self):
        stats = os.stat(self['file_path'])

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


# Export for API
Origin = Directory
