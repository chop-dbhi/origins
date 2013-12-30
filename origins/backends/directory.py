from __future__ import division, unicode_literals
from ..constants import DATETIME_FORMAT
from . import base, _file

import os
from datetime import datetime


class Client(base.Client):
    def __init__(self, path, **kwargs):
        self.directory_path = os.path.abspath(path)
        self.recurse = kwargs.get('recurse', True)

        # Explictly set depth to 0. A None depth implies no limit
        if not self.recurse:
            self.depth = 0
        else:
            self.depth = kwargs.get('depth')

        # TODO support regexp or glob syntax for matching a subset of files
        # self.regexp = kwargs.get('regexp')

    def directory(self):
        return {
            'path': self.directory_path,
            'name': os.path.basename(self.directory_path),
        }

    def files(self):
        _files = []

        for root, dirs, files in os.walk(self.directory_path):
            if self.depth is not None:
                curpath = os.path.relpath(root, self.directory_path)

                if curpath == '.':
                    depth = 0
                else:
                    depth = len(curpath.split(os.path.sep))

                # Remove all subdirectories from traversal once the
                # desired depth has been reached. Note a `break` does
                # not work since this would stop processing sibling
                # directories as well.
                if depth >= self.depth:
                    for _ in dirs[:]:
                        dirs.pop()

            for f in files:
                path = os.path.join(root, f)
                _files.append({
                    'name': os.path.relpath(path, self.directory_path),
                    'path': path,
                })

        return _files


class Directory(base.Node):
    def sync(self):
        self.update(self.client.directory())
        self._contains(self.client.files(), File)

    @property
    def files(self):
        return self._containers('file')


class File(_file.File):
    def sync(self):
        stats = os.stat(self['path'])

        # Convert into datetime from timestamp floats
        atime = datetime.fromtimestamp(stats.st_atime)
        mtime = datetime.fromtimestamp(stats.st_mtime)
        if hasattr(stats, 'st_birthtime'):
            create_time = stats.st_birthtime
        else:
            create_time = stats.st_ctime
        ctime = datetime.fromtimestamp(create_time)

        self.update({
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
