from flask.ext import restful
from .components import ComponentsResource
from .resources import ResourcesResource
from .relationships import RelationshipsResource


class SearchResource(restful.Resource):
    def get(self):
        handlers = {
            'components': ComponentsResource(),
            'resources': ResourcesResource(),
            'relationships': RelationshipsResource(),
        }

        output = {}

        for key in handlers:
            output[key] = handlers[key].get()

        return output
