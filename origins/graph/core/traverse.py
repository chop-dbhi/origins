from .. import utils, cypher


def match(statement, predicate=None, limit=None, skip=None,
          model=None, type=None):

    if predicate:
        placeholder = cypher.map(predicate.keys(), 'pred')
        parameters = {'pred': predicate}
    else:
        placeholder = ''
        parameters = {}

    statement = utils.prep(statement,
                           type=type,
                           model=model,
                           predicate=placeholder)

    if skip:
        statement += ' SKIP { skip }'
        parameters['skip'] = skip

    if limit:
        statement += ' LIMIT { limit }'
        parameters['limit'] = limit

    return {
        'statement': statement,
        'parameters': parameters,
    }


def search(statement, predicate, operator=None, limit=None, skip=None,
           model=None, type=None):

    if operator is None:
        operator = 'AND'

    placeholder = cypher.search(predicate,
                                variable='n',
                                parameter='predicate',
                                operator=operator)

    statement = utils.prep(statement,
                           type=type,
                           model=model,
                           predicate=placeholder)

    parameters = {
        'predicate': predicate,
    }

    if skip:
        statement += ' SKIP { skip }'
        parameters['skip'] = skip

    if limit:
        statement += ' LIMIT { limit }'
        parameters['limit'] = limit

    return {
        'statement': statement,
        'parameters': parameters,
    }
