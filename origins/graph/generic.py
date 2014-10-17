from . import neo4j, utils, models


REVISIONS_STATEMENT = '''
    MATCH ($model {`origins:uuid`: { uuid }})-[:`prov:usedEntity`|`prov:generatedEntity`*]-(d:`prov:Derivation` {`prov:type`: 'prov:Revision'})
    WITH d
    MATCH (d)-[:`prov:usedEntity`]->(n),
          (n)<-[:`prov:entity`]-(i:`prov:Invalidation`)
    RETURN n, i
'''  # noqa


EDGES_STATEMENT = '''
    MATCH ($model {`origins:uuid`: { uuid }})<-[:`origins:start`|`origins:end`]-(n:`origins:Edge`)
    WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
    WITH n
    MATCH (n)-[:`origins:start`]->(s),
          (n)-[:`origins:end`]->(e)
    RETURN n, null, s, e
'''  # noqa


TIMELINE_STATEMENT = '''
    MATCH ($model {`origins:uuid`: { uuid }})<-[r]-(p:`prov:Relation`)
    RETURN p, type(r)
    ORDER BY p.`origins:time` DESC
'''


def revisions(node, tx=neo4j.tx):
    statement = utils.prep(REVISIONS_STATEMENT,
                           model=node.model_name)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': node.uuid,
        }
    }

    result = [node]

    for n, i in tx.send(query):
        result.append(node.parse(n, i))

    return result


def edges(node, tx=neo4j.tx):
    statement = utils.prep(EDGES_STATEMENT,
                           model=node.model_name)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': node.uuid,
        }
    }

    result = []

    for n, i, s, e in tx.send(query):
        model = models[n['origins:model']]
        result.append(model.parse(n, i, s, e))

    return result


def timeline(node, tx=neo4j.tx):
    statement = utils.prep(TIMELINE_STATEMENT,
                           model=node.model_name)

    query = {
        'statement': statement,
        'parameters': {
            'uuid': node.uuid,
        }
    }

    return tx.send(query)
