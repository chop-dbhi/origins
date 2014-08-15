from __future__ import unicode_literals, absolute_import

import re
import time
import json
import uuid
import inspect
import logging
import requests
from importlib import import_module
from . import config
from .exceptions import OriginsError

try:
    import redis
except ImportError:
    redis = None

try:
    str = unicode
except NameError:
    pass


HTTP_RE = re.compile(r'^https?://', re.I)

logger = logging.getLogger(__name__)

# Shared, in-process Redis client
_redis_client = None


def get_redis_client():
    global _redis_client

    if config.options['events_enabled'] is False:
        logger.debug('events system disabled')
        return

    if not redis:
        if config.options['events_enabled'] is True:
            raise OriginsError('events enabled, but redis '
                               'library is not installed')

        logger.warning('redis library not installed, events disabled')
        return

    if not _redis_client:
        _redis_client = redis.StrictRedis(host=config.options['redis_host'],
                                          port=config.options['REDIS_PORT'],
                                          db=config.options['REDIS_DB'])

    return _redis_client


def publish_web(url, event, data):
    try:
        requests.post(url, data=json.dumps({'event': event, 'data': data}),
                      headers={'Content-Type': 'application/json'})
    except requests.ConnectionError:
        pass


def publish_func(func, event, data):
    if isinstance(func, (str, bytes)):
        func = import_func(func)

    func(event, data)


def import_func(path):
    "Imports and returns a function given it's import path."
    toks = path.split('.')

    func_name = toks.pop()

    module_name = '.'.join(toks)
    module = import_module(module_name)

    return getattr(module, func_name)


def handler_path(handler):
    if inspect.isfunction(handler):
        return '{}.{}'.format(handler.__module__, handler.__name__)

    return handler


def make_test_payload():
    return {
        'topic': 'origins',
        'uuid': str(uuid.uuid4()),
        'timestamp': int(time.time() * 1000),
    }, None


def test_web(url):
    "Triggers a test payload to the URL."
    event, data = make_test_payload()
    publish_web(event, data)


def test_func(func):
    "Triggers a test payload to the function handler."
    event, data = make_test_payload()
    publish_func(func, event, data)


def reset():
    "Resets the redis database."
    r = get_redis_client()

    if not r:
        return

    r.flushdb()


def register_event(topic):
    r = get_redis_client()

    if not r:
        return

    r.sadd(topic)


def unregister_event(topic):
    r = get_redis_client()

    if not r:
        return

    r.srem(topic)


def queue(topic, data=None):
    "Queues an event paload for the specified topic."
    r = get_redis_client()

    if not r:
        return

    key = 'events.{}'.format(topic)

    value = {
        'event': {
            'topic': topic,
            'uuid': str(uuid.uuid4()),
            'timestamp': int(time.time() * 1000),
        },
        'data': data,
    }

    r.rpush(key, json.dumps(value))


def dequeue(topic):
    "Dequeues events for the given topic."
    r = get_redis_client()

    if not r:
        return

    key = 'events.{}'.format(topic)

    value = r.rpop(key)

    # Nothing in the events queue
    if value is None:
        return

    value = json.loads(value)

    event = value['event']
    topic = event['topic']

    data = value['data']

    # Subscriptions for this event
    key = 'subscribers.{}'.format(topic)

    handlers = r.smembers(key)

    if handlers:
        # TODO add to secondary queue for distributed work. For now
        # subscriber handlers will be executed in sequence.
        for handler in handlers:
            if HTTP_RE.match(handler):
                publish_web(handler, event, data)
            else:
                publish_func(handler, event, data)

    # Event contains object UUID. Check for watchers
    if data and 'uuid' in data:
        key = 'watchers.{}'.format(topic, data['uuid'])

        handlers = r.smembers(key)

        if handlers:
            # TODO add to secondary queue for distributed work. For now
            # watchers handlers will be executed in sequence.
            for handler in handlers:
                if HTTP_RE.match(handler):
                    publish_web(handler, event, data)
                else:
                    publish_func(handler, event, data)


# Public methods

def subscribe(topic, handler):
    """Subscribes to a topic given a handler.

    `handler` can be an Python function, import path to a function,
    or an HTTP URL for use as a webhook.
    """
    r = get_redis_client()

    if not r:
        return

    if inspect.isfunction(handler):
        handler = '{}.{}'.format(handler.__module__, handler.__name__)

    # List of in-code subscriptions for this event
    key = 'subscribers.{}'.format(topic)

    r.sadd(key, handler)


def unsubscribe(topic, handler):
    r = get_redis_client()

    if not r:
        return

    key = 'subscribers.{}'.format(topic)

    r.srem(key, handler)


def watch(uuid, handler):
    """Subscribes a watch handler for an object.

    All events that involve the watched object will be passed into the handler.
    """
    r = get_redis_client()

    if not r:
        return

    if inspect.isfunction(handler):
        handler = '{}.{}'.format(handler.__module__, handler.__name__)

    key = 'watchers.{}'.format(uuid)

    r.sadd(key, handler)


def unwatch(uuid, handler):
    r = get_redis_client()

    if not r:
        return

    key = 'watchers.{}'.format(uuid)

    r.srem(key, handler)
