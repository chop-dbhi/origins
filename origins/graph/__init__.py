from .model import Continuant  # noqa
from .nodes import Node  # noqa
from .edges import Edge  # noqa
from .resources import Resource  # noqa
from .components import Component  # noqa
from .relationships import Relationship  # noqa
from .collections import Collection  # noqa


_models = {
    Continuant,
    Node,
    Edge,
    Resource,
    Component,
    Relationship,
    Collection,
}

# Map of model names to their models
models = {m.model_name: m for m in _models}
