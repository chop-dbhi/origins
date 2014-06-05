from __future__ import unicode_literals, absolute_import

import time
from uuid import uuid4
from hashlib import sha1
from .cypher import cypher_map
from .identifier import QualifiedName
from .constants import *  # noqa

try:
    str = unicode
except NameError:
    pass


class ValidationError(Exception):
    pass


def _sha1(props):
    """Computes the SHA1 hash of the passed properties.

    Origins namespaced properties are ignored.
    """
    toks = []

    for key, value in props.items():
        # Ignore Origins-defined properties
        if isinstance(key, QualifiedName):
            if key.namespace is ORIGINS:
                continue
            key = str(key).encode('utf8')
        elif isinstance(key, str):
            key = key.encode('utf8')

        if isinstance(value, QualifiedName):
            value = str(value).encode('utf8')
        elif isinstance(value, str):
            value = value.encode('utf8')

        toks.append(b'{}|{}'.format(key, value))

    if toks:
        data = b','.join(sorted(toks))
        return sha1(data).hexdigest()


class Interface(object):
    object_type = None
    object_uid_attr = ORIGINS_ATTR_UUID

    is_relation = False
    is_event = False

    need_sha1 = False

    def __init__(self, props=None, timestamp=None):
        if props is None:
            props = {}

        props[ORIGINS_ATTR_SHA1] = _sha1(props)
        props[ORIGINS_ATTR_TYPE] = self.object_type

        if self.object_uid_attr is ORIGINS_ATTR_UUID \
                and ORIGINS_ATTR_UUID not in props:
            props[ORIGINS_ATTR_UUID] = str(uuid4())

        if ORIGINS_ATTR_TIMESTAMP not in props:
            if timestamp is None:
                timestamp = int(time.time() * 1000)

            props[ORIGINS_ATTR_TIMESTAMP] = timestamp

        self.props = props

    def _node(self, props, labels=None, identifier=None):
        if labels is None:
            labels = [self.object_type]

            if self.is_relation:
                labels.append(PROV_RELATION)

                if self.is_event:
                    labels.append(PROV_EVENT)

        label_strings = ('`' + str(l) + '`' for l in labels)

        return '({}:{} {})'.format(identifier, ':'.join(label_strings),
                                   cypher_map(props))

    def node(self, identifier=None):
        "Returns a node containing all properties."
        return self._node(self.props, identifier=identifier)

    def match_node(self, identifier=None):
        "Returns a node that uniquely identifies the node."
        props = {self.object_uid_attr: self.props[self.object_uid_attr]}
        return self._node(props, [self.object_type], identifier)


class Namespace(Interface):
    object_type = ORIGINS_NAMESPACE
    object_uid_attr = ORIGINS_ATTR_URI


class Bundle(Interface):
    object_type = PROV_BUNDLE


class Document(Interface):
    object_type = ORIGINS_DOCUMENT


class Entity(Interface):
    object_type = PROV_ENTITY


class Activity(Interface):
    object_type = PROV_ACTIVITY


class Agent(Interface):
    object_type = PROV_AGENT


# Base class for relations
class Relation(Interface):
    is_relation = True

    relations = {}

    def node(self, identifier=None):
        # Remove relation-based properties
        props = {k: v for k, v in self.props.items()
                 if k not in self.relations}
        return self._node(props, identifier=identifier)


# Base class for event-based relations
class Event(Relation):
    is_event = True


class Generation(Event):
    object_type = PROV_GENERATION

    relations = {
        PROV_ATTR_ENTITY: True,
        PROV_ATTR_ACTIVITY: False,
    }


class Usage(Event):
    object_type = PROV_USAGE

    relations = {
        PROV_ATTR_ACTIVITY: True,
        PROV_ATTR_ENTITY: False,
    }


class Communication(Relation):
    object_type = PROV_COMMUNICATION

    relations = {
        PROV_ATTR_INFORMED: True,
        PROV_ATTR_INFORMANT: False,
    }


class Start(Event):
    object_type = PROV_START

    relations = {
        PROV_ATTR_ACTIVITY: True,
        PROV_ATTR_TRIGGER: False,
        PROV_ATTR_STARTER: False,
    }


class End(Event):
    object_type = PROV_END

    relations = {
        PROV_ATTR_ACTIVITY: True,
        PROV_ATTR_TRIGGER: False,
        PROV_ATTR_ENDER: False,
    }


class Invalidation(Event):
    object_type = PROV_INVALIDATION

    relations = {
        PROV_ATTR_ENTITY: True,
        PROV_ATTR_ACTIVITY: False,
    }


class Derivation(Relation):
    object_type = PROV_DERIVATION

    relations = {
        PROV_ATTR_GENERATED_ENTITY: True,
        PROV_ATTR_USED_ENTITY: True,
        PROV_ATTR_ACTIVITY: False,
        PROV_ATTR_GENERATION: False,
        PROV_ATTR_USAGE: False,
    }


class Attribution(Relation):
    object_type = PROV_ATTRIBUTION

    relations = {
        PROV_ATTR_ENTITY: True,
        PROV_ATTR_AGENT: True,
    }


class Association(Relation):
    object_type = PROV_ASSOCIATION

    relations = {
        PROV_ATTR_ACTIVITY: True,
        PROV_ATTR_AGENT: False,
        PROV_ATTR_PLAN: False,
    }


class Delegation(Relation):
    object_type = PROV_DELEGATION

    relations = {
        PROV_ATTR_DELEGATE: True,
        PROV_ATTR_RESPONSIBLE: True,
        PROV_ATTR_ACTIVITY: False,
    }


class Influence(Relation):
    object_type = PROV_INFLUENCE

    relations = {
        PROV_ATTR_INFLUENCEE: True,
        PROV_ATTR_INFLUENCER: True,
    }


class Specialization(Relation):
    object_type = PROV_SPECIALIZATION

    relations = {
        PROV_ATTR_SPECIFIC_ENTITY: True,
        PROV_ATTR_GENERAL_ENTITY: True,
    }


class Alternate(Relation):
    object_type = PROV_ALTERNATE

    relations = {
        PROV_ATTR_ALTERNATE1: True,
        PROV_ATTR_ALTERNATE2: True,
    }


class Mention(Relation):
    object_type = PROV_MENTION

    relations = {
        PROV_ATTR_SPECIFIC_ENTITY: True,
        PROV_ATTR_GENERAL_ENTITY: True,
        PROV_ATTR_BUNDLE: True,
    }


class Membership(Relation):
    object_type = PROV_MEMBERSHIP

    relations = {
        PROV_ATTR_COLLECTION: True,
        PROV_ATTR_ENTITY: True,
    }


COMPONENT_TYPES = {
    PROV: {
        PROV_BUNDLE: Bundle,
        PROV_ENTITY: Entity,
        PROV_ACTIVITY: Activity,
        PROV_AGENT: Agent,
        PROV_GENERATION: Generation,
        PROV_USAGE: Usage,
        PROV_COMMUNICATION: Communication,
        PROV_START: Start,
        PROV_END: End,
        PROV_INVALIDATION: Invalidation,
        PROV_DERIVATION: Derivation,
        PROV_ATTRIBUTION: Attribution,
        PROV_ASSOCIATION: Association,
        PROV_DELEGATION: Delegation,
        PROV_INFLUENCE: Influence,
        PROV_SPECIALIZATION: Specialization,
        PROV_ALTERNATE: Alternate,
        PROV_MENTION: Mention,
        PROV_MEMBERSHIP: Membership,
    },
}

# Map PROV-N notation
for provn in PROV_N_MAP:
    COMPONENT_TYPES[PROV][provn] = COMPONENT_TYPES[PROV][PROV_N_MAP[provn]]
