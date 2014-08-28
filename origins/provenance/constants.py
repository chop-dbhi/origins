"""
Provenance types as defined in the W3C PROV specification. Also included
are sub-types defined by Origins.
"""

from .identifier import Namespaces


NAMESPACES = Namespaces({
    'xsd': 'http://www.w3.org/2000/10/XMLSchema#',
    'prov': 'http://www.w3.org/ns/prov#',
    'up': 'http://semweb.mmlab.be/ns/up#',
    'origins': 'http://origins.link/1.0/',
})


PROV = NAMESPACES['prov']
UP = NAMESPACES['up']
ORIGINS = NAMESPACES['origins']

# Entities/Activities
PROV_ENTITY = PROV['Entity']
PROV_ACTIVITY = PROV['Activity']
PROV_GENERATION = PROV['Generation']
PROV_USAGE = PROV['Usage']
PROV_COMMUNICATION = PROV['Communication']
PROV_START = PROV['Start']
PROV_END = PROV['End']
PROV_INVALIDATION = PROV['Invalidation']

# General label for relations and events
PROV_RELATION = PROV['Relation']
PROV_EVENT = PROV['Event']

# Derivations
PROV_DERIVATION = PROV['Derivation']

# Agents/Responsibility
PROV_AGENT = PROV['Agent']
PROV_ATTRIBUTION = PROV['Attribution']
PROV_ASSOCIATION = PROV['Association']
PROV_DELEGATION = PROV['Delegation']
PROV_INFLUENCE = PROV['Influence']

# Bundles
PROV_BUNDLE = PROV['Bundle']

# Alternate
PROV_ALTERNATE = PROV['Alternate']
PROV_SPECIALIZATION = PROV['Specialization']
PROV_MENTION = PROV['Mention']

# Collections
PROV_MEMBERSHIP = PROV['Membership']

# Maps PROV-N components to standard PROV components
PROV_ALT_COMPONENTS = {
    PROV['entity']: PROV_ENTITY,
    PROV['agent']: PROV_AGENT,
    PROV['activity']: PROV_ACTIVITY,
    PROV['bundle']: PROV_BUNDLE,
    PROV['wasGeneratedBy']: PROV_GENERATION,
    PROV['used']: PROV_USAGE,
    PROV['wasInformedBy']: PROV_COMMUNICATION,
    PROV['wasStartedBy']: PROV_START,
    PROV['wasEndedBy']: PROV_END,
    PROV['wasInvalidatedBy']: PROV_INVALIDATION,
    PROV['wasDerivedFrom']: PROV_DERIVATION,
    PROV['wasAttributedTo']: PROV_ATTRIBUTION,
    PROV['wasAssociatedWith']: PROV_ASSOCIATION,
    PROV['actedOnBehalfOf']: PROV_DELEGATION,
    PROV['wasInfluencedBy']: PROV_INFLUENCE,
    PROV['alternateOf']: PROV_ALTERNATE,
    PROV['specializationOf']: PROV_SPECIALIZATION,
    PROV['mentionOf']: PROV_MENTION,
    PROV['hadMember']: PROV_MEMBERSHIP,
}

# Identifiers for PROV's attributes
PROV_ATTR_ENTITY = PROV['entity']
PROV_ATTR_ACTIVITY = PROV['activity']
PROV_ATTR_TRIGGER = PROV['trigger']
PROV_ATTR_INFORMED = PROV['informed']
PROV_ATTR_INFORMANT = PROV['informant']
PROV_ATTR_STARTER = PROV['starter']
PROV_ATTR_ENDER = PROV['ender']
PROV_ATTR_AGENT = PROV['agent']
PROV_ATTR_PLAN = PROV['plan']
PROV_ATTR_DELEGATE = PROV['delegate']
PROV_ATTR_RESPONSIBLE = PROV['responsible']
PROV_ATTR_GENERATED_ENTITY = PROV['generatedEntity']
PROV_ATTR_USED_ENTITY = PROV['usedEntity']
PROV_ATTR_GENERATION = PROV['generation']
PROV_ATTR_USAGE = PROV['usage']
PROV_ATTR_SPECIFIC_ENTITY = PROV['specificEntity']
PROV_ATTR_GENERAL_ENTITY = PROV['generalEntity']
PROV_ATTR_ALTERNATE1 = PROV['alternate1']
PROV_ATTR_ALTERNATE2 = PROV['alternate2']
PROV_ATTR_BUNDLE = PROV['bundle']
PROV_ATTR_INFLUENCEE = PROV['influencee']
PROV_ATTR_INFLUENCER = PROV['influencer']
PROV_ATTR_COLLECTION = PROV['collection']

# Literal properties
PROV_ATTR_ID = PROV['id']
PROV_ATTR_TIME = PROV['time']
PROV_ATTR_STARTTIME = PROV['startTime']
PROV_ATTR_ENDTIME = PROV['endTime']

# Pre-defined attributes
PROV_ATTR_TYPE = PROV['type']
PROV_ATTR_LABEL = PROV['label']
PROV_ATTR_VALUE = PROV['value']
PROV_ATTR_ROLE = PROV['role']
PROV_ATTR_LOCATION = PROV['location']

# PROV entity types
PROV_TYPE_REVISION = PROV['Revision']
PROV_TYPE_QUOTATION = PROV['Quotation']
PROV_TYPE_PRIMARY_SOURCE = PROV['PrimarySouce']
PROV_TYPE_COLLECTION = PROV['Collection']
PROV_TYPE_EMPTY_COLLECTION = PROV['EmptyCollection']
PROV_TYPE_PLAN = PROV['Plan']
PROV_TYPE_BUNDLE = PROV['Bundle']

# PROV agent types
PROV_TYPE_PERSON = PROV['Person']
PROV_TYPE_ORGANIZATION = PROV['Organization']
PROV_TYPE_SOFTWARE_AGENT = PROV['SoftwareAgent']


PROV_EVENT_TYPES = {
    PROV_GENERATION,
    PROV_USAGE,
    PROV_START,
    PROV_END,
    PROV_INVALIDATION,
}

PROV_RELATION_TYPES = PROV_EVENT_TYPES | {
    PROV_COMMUNICATION,
    PROV_DERIVATION,
    PROV_ATTRIBUTION,
    PROV_ASSOCIATION,
    PROV_DELEGATION,
    PROV_INFLUENCE,
    PROV_SPECIALIZATION,
    PROV_ALTERNATE,
    PROV_MENTION,
    PROV_MEMBERSHIP,
}

PROV_OBJECT_TYPES = {
    PROV_ENTITY,
    PROV_ACTIVITY,
    PROV_AGENT,
    PROV_BUNDLE,
}

PROV_COMPONENTS = PROV_OBJECT_TYPES | PROV_RELATION_TYPES


PROV_RELATION_ATTRS = {
    PROV_GENERATION: {
        PROV_ATTR_ENTITY: True,
        PROV_ATTR_ACTIVITY: False,
    },

    PROV_USAGE: {
        PROV_ATTR_ACTIVITY: True,
        PROV_ATTR_ENTITY: False,
    },

    PROV_COMMUNICATION: {
        PROV_ATTR_INFORMED: True,
        PROV_ATTR_INFORMANT: False,
    },

    PROV_START: {
        PROV_ATTR_ACTIVITY: True,
        PROV_ATTR_TRIGGER: False,
        PROV_ATTR_STARTER: False,
    },

    PROV_END: {
        PROV_ATTR_ACTIVITY: True,
        PROV_ATTR_TRIGGER: False,
        PROV_ATTR_ENDER: False,
    },

    PROV_INVALIDATION: {
        PROV_ATTR_ENTITY: True,
        PROV_ATTR_ACTIVITY: False,
    },

    PROV_DERIVATION: {
        PROV_ATTR_GENERATED_ENTITY: True,
        PROV_ATTR_USED_ENTITY: True,
        PROV_ATTR_ACTIVITY: False,
        PROV_ATTR_GENERATION: False,
        PROV_ATTR_USAGE: False,
    },

    PROV_ATTRIBUTION: {
        PROV_ATTR_ENTITY: True,
        PROV_ATTR_AGENT: True,
    },

    PROV_ASSOCIATION: {
        PROV_ATTR_ACTIVITY: True,
        PROV_ATTR_AGENT: False,
        PROV_ATTR_PLAN: False,
    },

    PROV_DELEGATION: {
        PROV_ATTR_DELEGATE: True,
        PROV_ATTR_RESPONSIBLE: True,
        PROV_ATTR_ACTIVITY: False,
    },

    PROV_INFLUENCE: {
        PROV_ATTR_INFLUENCEE: True,
        PROV_ATTR_INFLUENCER: True,
    },

    PROV_SPECIALIZATION: {
        PROV_ATTR_SPECIFIC_ENTITY: True,
        PROV_ATTR_GENERAL_ENTITY: True,
    },

    PROV_ALTERNATE: {
        PROV_ATTR_ALTERNATE1: True,
        PROV_ATTR_ALTERNATE2: True,
    },

    PROV_MENTION: {
        PROV_ATTR_SPECIFIC_ENTITY: True,
        PROV_ATTR_GENERAL_ENTITY: True,
        PROV_ATTR_BUNDLE: True,
    },

    PROV_MEMBERSHIP: {
        PROV_ATTR_COLLECTION: True,
        PROV_ATTR_ENTITY: True,
    },
}


# UP attributes
UP_ATTR_ASSERTION_CONFIDENCE = UP['assertionConfidence']
UP_ATTR_CONTENT_CONFIDENCE = UP['contentConfidence']
UP_ATTR_ASSERTION_TYPE = UP['assertionType']

# UP types
UP_TYPE_HUMAN_ASSERTED = UP['HumanAsserted']
UP_TYPE_MACHINE_GENERATED = UP['MachineGenerated']
UP_TYPE_MACHINE_COLLECTED = UP['MachineCollected']
UP_TYPE_COMPLETE = UP['Complete']
UP_TYPE_INCOMPLETE = UP['Incomplete']
UP_TYPE_FUTURE = UP['Future']
UP_TYPE_TRUSTED = UP['Trusted']
UP_TYPE_UNTRUSTED = UP['Untrusted']

ORIGINS_PROVENANCE = ORIGINS['Provenance']
ORIGINS_RESOURCE = ORIGINS['Resource']
ORIGINS_REPOSITORY = ORIGINS['Repository']
ORIGINS_NAMESPACE = ORIGINS['Namespace']
ORIGINS_DOCUMENT = ORIGINS['Document']
ORIGINS_DEPENDENCE = ORIGINS['Dependence']

# Origins general attributes
ORIGINS_ATTR_ID = ORIGINS['id']
ORIGINS_ATTR_UUID = ORIGINS['uuid']
ORIGINS_ATTR_TIME = ORIGINS['time']
ORIGINS_ATTR_TYPE = ORIGINS['type']
ORIGINS_ATTR_URI = ORIGINS['uri']
ORIGINS_ATTR_PREFIX = ORIGINS['prefix']
ORIGINS_ATTR_NEO4J = ORIGINS['neo4j']

# Dependence attributes
ORIGINS_ATTR_DEPENDENT = ORIGINS['dependent']
ORIGINS_ATTR_DEPENDENCY = ORIGINS['dependency']

ORIGINS_REL_SEMANTICS = ORIGINS['hadSemanticsFrom']
ORIGINS_REL_DESCRIPTION = ORIGINS['hadDescription']

ORIGINS_OBJECT_TYPES = set()

ORIGINS_EVENT_TYPES = set()

ORIGINS_RELATION_TYPES = {
    ORIGINS_DEPENDENCE,
}

ORIGINS_ALT_COMPONENTS = {
    ORIGINS['wasDependentOn']: ORIGINS_DEPENDENCE,
}

ORIGINS_RELATION_ATTRS = {
    ORIGINS_DEPENDENCE: {
        ORIGINS_ATTR_DEPENDENT: True,
        ORIGINS_ATTR_DEPENDENCY: True,
    }
}

ORIGINS_COMPONENTS = ORIGINS_OBJECT_TYPES | ORIGINS_RELATION_TYPES

# All configuration options for supported namespaces
OBJECT_TYPES = PROV_OBJECT_TYPES | ORIGINS_OBJECT_TYPES
RELATION_TYPES = PROV_RELATION_TYPES | ORIGINS_RELATION_TYPES
EVENT_TYPES = PROV_EVENT_TYPES | ORIGINS_EVENT_TYPES

COMPONENTS = PROV_COMPONENTS | ORIGINS_COMPONENTS

RELATION_ATTRS = {}
RELATION_ATTRS.update(PROV_RELATION_ATTRS)
RELATION_ATTRS.update(ORIGINS_RELATION_ATTRS)

ALT_COMPONENTS = {}
ALT_COMPONENTS.update(PROV_ALT_COMPONENTS)
ALT_COMPONENTS.update(ORIGINS_ALT_COMPONENTS)
