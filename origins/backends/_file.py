import os
from . import base


class Client(base.Client):
    def __init__(self, path, **kwargs):
        self.file_path = os.path.abspath(path)

    @property
    def file_name(self):
        return os.path.basename(self.file_path)

    @property
    def file_handler(self):
        return open(self.file_path)

    def file(self):
        return {
            'path': self.file_path,
            'name': self.file_name,
        }


class File(base.Node):
    def sync(self):
        self.update(self.client.file())
