import json
import uuid
import inspect
import logging
from functools import wraps
from ..utils import timestamp
from ._redis import client as redis
from . import utils

logger = logging.getLogger(__name__)


def _make_event(topic, data):
    return {
        'event': {
            'topic': topic,
            'uuid': str(uuid.uuid4()),
            'timestamp': timestamp(),
        },
        'data': data,
    }


def dequeue(topic):
    "Dequeues published events for the given topic."
    if not redis:
        return

    key = '{}.{}'.format(utils.EVENTS, topic)

    value = redis.rpop(key)

    # Nothing in the events queue
    if value is None:
        return

    value = json.loads(value)

    event = value['event']
    topic = event['topic']

    data = value['data']

    # Subscriptions for this event
    key = '{}.{}'.format(utils.SUBSCRIBERS, topic)

    handlers = redis.smembers(key)

    subscribers = len(handlers)
    watchers = None

    if handlers:
        # TODO add to secondary queue for distributed work. For now
        # subscriber handlers will be executed in sequence.
        for handler in handlers:
            if utils.HTTP_RE.match(handler):
                utils.publish_web(handler, event, data)
            else:
                utils.publish_func(handler, event, data)

    # Event contains object UUID. Check for watchers
    if data and 'uuid' in data:
        key = '{}.{}'.format(utils.WATCHERS, data['uuid'])

        handlers = redis.smembers(key)
        watchers = len(handlers)

        if handlers:
            # TODO add to secondary queue for distributed work. For now
            # watchers handlers will be executed in sequence.
            for handler in handlers:
                if utils.HTTP_RE.match(handler):
                    utils.publish_web(handler, event, data)
                else:
                    utils.publish_func(handler, event, data)

    return subscribers, watchers


# Public methods

def publish(topic, data=None, multi=True):
    "Publishes an event for the specified topic."
    if not redis:
        return

    # Ensure the topic is registered
    utils.register_topic(topic)

    key = '{}.{}'.format(utils.EVENTS, topic)

    if multi and isinstance(data, (list, tuple)):
        for value in data:
            redis.rpush(key, json.dumps(_make_event(topic, value)))
    else:
        redis.rpush(key, json.dumps(_make_event(topic, data)))


def subscribe(topic, handler):
    """Subscribes to a topic given a handler.

    `handler` can be an Python function, import path to a function,
    or an HTTP URL for use as a webhook.
    """
    if not redis:
        return

    if inspect.isfunction(handler):
        handler = '{}.{}'.format(handler.__module__, handler.__name__)

    utils.register_topic(topic)

    # List of in-code subscriptions for this event
    key = '{}.{}'.format(utils.SUBSCRIBERS, topic)

    redis.sadd(key, handler)


def unsubscribe(topic, handler):
    if not redis:
        return

    key = '{}.{}'.format(utils.SUBSCRIBERS, topic)

    redis.srem(key, handler)


def watch(uuid, handler):
    """Subscribes a watch handler for an object.

    All events that involve the watched object will be passed into the handler.
    """
    if not redis:
        return

    if inspect.isfunction(handler):
        handler = '{}.{}'.format(handler.__module__, handler.__name__)

    key = '{}.{}'.format(utils.WATCHERS, uuid)

    redis.sadd(key, handler)


def unwatch(uuid, handler):
    if not redis:
        return

    key = '{}.{}'.format(utils.WATCHERS, uuid)

    redis.srem(key, handler)
