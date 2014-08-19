import logging
from origins import config
from origins.exceptions import OriginsError

logger = logging.getLogger(__name__)

try:
    import redis
except ImportError:
    redis = None

# Share in-process client
client = None

if config.options['events_enabled'] is False:
    logger.debug('events system disabled')
elif not redis:
    if config.options['events_enabled'] is True:
        raise OriginsError('events enabled, but redis '
                           'library is not installed')

    logger.warning('redis library not installed, events disabled')

else:
    client = redis.StrictRedis(host=config.options['redis_host'],
                               port=config.options['redis_port'],
                               db=config.options['redis_db'],
                               decode_responses=True)
