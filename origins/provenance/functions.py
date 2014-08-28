from .. import utils


def add(entity, new=False):
    """Produces provenance data for general add operation.

    If `new` is true, the entity is treated as previously not existing and
    therefore receives a generation time.
    """
    prov = {
        'entity': {
            'entity': {
                'origins:uuid': entity,
            },
        },
        'wasGeneratedBy': {
            'wgb': {
                'prov:entity': 'entity',
            },
        },
    }

    # New object, mark it's generation time
    if new:
        timestamp = utils.timestamp()
        prov['wasGeneratedBy']['wgb']['prov:time'] = timestamp

    return prov


def change(previous, entity, method=None, reason=None):
    if not method:
        method = 'prov:Revision'

    if not reason:
        reason = 'origins:AttributeChange'

    prov = {
        'entity': {
            'entity': {
                'origins:uuid': entity,
            },
            'previous': {
                'origins:uuid': previous,
            }
        },
        'wasInvalidatedBy': {
            'wib': {
                'prov:entity': 'previous',
                'origins:reason': reason,
            },
        },
        'wasDerivedFrom': {
            'wdf': {
                'prov:type': method,
                'prov:usedEntity': 'previous',
                'prov:generatedEntity': 'entity',
            },
        },
    }

    return prov


def remove(entity, reason=None):
    if not reason:
        reason = 'origins:Remove'

    prov = {
        'entity': {
            'entity': {
                'origins:uuid': entity,
            },
        },
        'wasInvalidatedBy': {
            'wib': {
                'prov:entity': 'entity',
                'origins:reason': reason,
            },
        },
    }

    return prov
