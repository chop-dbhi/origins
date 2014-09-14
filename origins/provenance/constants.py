PROV_RELATIONS = {
    'wasGeneratedBy': {
        'prov:entity': True,
        'prov:activity': False,
    },

    'used': {
        'prov:activity': True,
        'prov:entity': False,
    },

    'wasInformedBy': {
        'prov:informed': True,
        'prov:informant': False,
    },

    'wasStartedBy': {
        'prov:activity': True,
        'prov:trigger': False,
        'prov:starter': False,
    },

    'wasEndedBy': {
        'prov:activity': True,
        'prov:trigger': False,
        'prov:ender': False,
    },

    'wasInvalidatedBy': {
        'prov:entity': True,
        'prov:activity': False,
    },

    'wasDerivedFrom': {
        'prov:generatedEntity': True,
        'prov:usedEntity': True,
        'prov:activity': False,
        'prov:generation': False,
        'prov:usage': False,
    },

    'wasAttributedTo': {
        'prov:entity': True,
        'prov:agent': True,
    },

    'wasAssociatedWith': {
        'prov:activity': True,
        'prov:agent': False,
        'prov:plan': False,
    },

    'actedOnBehalfOf': {
        'prov:delegate': True,
        'prov:responsible': True,
        'prov:activity': False,
    },

    'wasInfluencedBy': {
        'prov:influencee': True,
        'prov:influencer': True,
    },

    'specializationOf': {
        'prov:specificEntity': True,
        'prov:generalEntity': True,
    },

    'alternateOf': {
        'prov:alternate1': True,
        'prov:alternate2': True,
    },

    'mentionOf': {
        'prov:specificEntity': True,
        'prov:generalEntity': True,
        'prov:bundle': True,
    },

    'hadMember': {
        'prov:collection': True,
        'prov:entity': True,
    },
}

PROV_TYPES = {
    'entity': 'prov:Entity',
    'activity': 'prov:Activity',
    'agent': 'prov:Agent',
    'bundle': 'prov:Bundle',
    'wasGeneratedBy': 'prov:Generation',
    'used': 'prov:Usage',
    'wasInformedBy': 'prov:Communication',
    'wasStartedBy': 'prov:Start',
    'wasEndedBy': 'prov:End',
    'wasInvalidatedBy': 'prov:Invalidation',
    'wasDerivedFrom': 'prov:Derivation',
    'wasAttributedTo': 'prov:Attribution',
    'wasAssociatedWith': 'prov:Association',
    'actedOnBehalfOf': 'prov:Delegation',
    'wasInfluencedBy': 'prov:Influence',
    'specializationOf': 'prov:Specialization',
    'alternateOf': 'prov:Alternate',
    'mentionOf': 'prov:Mention',
    'hadMember': 'prov:Membership',
}

PROV_EVENTS = {
    'wasGeneratedBy',
    'used',
    'wasStartedBy',
    'wasEndedBy',
    'wasInvalidatedBy',
}

PROV_TERMS = {'entity', 'activity', 'agent', 'bundle'} | set(PROV_RELATIONS)
