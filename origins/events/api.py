import json
import uuid
import time
import inspect
import logging
from redis import StrictRedis
from origins import config
from . import utils


__all__ = (
    'publish',
    'subscribe',
    'unsubscribe',
    'watch',
    'unwatch',
    'purge',
    'debug',
    'client',
    'logger',
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

client = StrictRedis(decode_responses=True, **config.options['redis'])


def _make_event(topic, data):
    return {
        'event': {
            'topic': topic,
            'uuid': str(uuid.uuid4()),
            'time': int(time.time() * 1000),
        },
        'data': data,
    }


def _register_topic(topic):
    client.sadd(utils.TOPICS_KEY, topic)


# Public methods

def publish(topic, data):
    "Publishes an event for the specified topic."
    # Ensure the topic is registered
    _register_topic(topic)

    key = '{}.{}'.format(utils.EVENTS_KEY, topic)

    client.rpush(key, json.dumps(_make_event(topic, data)))

    logger.debug('published event to "{}"'.format(key))


def subscribe(topic, handler):
    """Subscribes to a topic given a handler.

    `handler` can be an Python function, import path to a function,
    or an HTTP URL for use as a webhook.
    """
    if inspect.isfunction(handler):
        handler = '{}.{}'.format(handler.__module__, handler.__name__)

    _register_topic(topic)

    # List of in-code subscriptions for this event
    key = '{}.{}'.format(utils.SUBSCRIBERS_KEY, topic)

    client.sadd(key, handler)

    logger.debug('subscribed to "{}"'.format(topic))


def unsubscribe(topic, handler):
    key = '{}.{}'.format(utils.SUBSCRIBERS_KEY, topic)

    client.srem(key, handler)

    logger.debug('unsubscribed from "{}"'.format(topic))


def watch(uuid, handler):
    """Subscribes a watch handler for an object.

    All events that involve the watched object will be passed into the handler.
    """
    if inspect.isfunction(handler):
        handler = '{}.{}'.format(handler.__module__, handler.__name__)

    key = '{}.{}'.format(utils.WATCHERS_KEY, uuid)

    client.sadd(key, handler)

    logger.debug('started watching "{}"'.format(uuid))


def unwatch(uuid, handler):
    key = '{}.{}'.format(utils.WATCHERS_KEY, uuid)

    client.srem(key, handler)

    logger.debug('stopped watching "{}"'.format(uuid))


def purge():
    "Resets the redis database."
    client.flushdb()


def debug():
    logger.setLevel(logging.DEBUG)


if config.options['debug']:
    debug()
