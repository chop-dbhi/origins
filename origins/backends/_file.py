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


class Node(base.Node):
    label_attribute = 'file_name'
    path_attribute = 'file_path'

    def synchronize(self):
        self.attrs['file_path'] = self.client.file_path
        self.attrs['file_name'] = self.client.file_name
