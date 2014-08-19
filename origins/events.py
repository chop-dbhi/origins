import re
import sys
import time
import json
import uuid
import inspect
import logging
import requests
import traceback
from datetime import datetime
from concurrent import futures
from multiprocessing import cpu_count
from importlib import import_module
from . import config
from .exceptions import OriginsError

try:
    import redis
except ImportError:
    redis = None

HTTP_RE = re.compile(r'^https?://', re.I)

logger = logging.getLogger(__name__)

# Shared, in-process Redis client
_redis_client = None


ORIGINS_TOPICS = 'origins.topics'
ORIGINS_SUBSCRIBERS = 'origins.subscribers'
ORIGINS_WATCHERS = 'origins.watchers'
ORIGINS_EVENTS = 'origins.events'


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
                                          port=config.options['redis_port'],
                                          db=config.options['redis_db'],
                                          decode_responses=True)

    return _redis_client


def publish_web(url, event, data):
    try:
        requests.post(url, data=json.dumps({
            'event': event,
            'data': data,
        }), headers={
            'Content-Type': 'application/json',
        })
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


def register_topic(topic):
    r = get_redis_client()

    if not r:
        return

    r.sadd(ORIGINS_TOPICS, topic)


def queue(topic, data=None):
    "Queues an event paload for the specified topic."
    r = get_redis_client()

    if not r:
        return

    # Ensure the topic is registered
    register_topic(topic)

    key = '{}.{}'.format(ORIGINS_EVENTS, topic)

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

    key = '{}.{}'.format(ORIGINS_EVENTS, topic)

    value = r.rpop(key)

    # Nothing in the events queue
    if value is None:
        return

    value = json.loads(value)

    event = value['event']
    topic = event['topic']

    data = value['data']

    # Subscriptions for this event
    key = '{}.{}'.format(ORIGINS_SUBSCRIBERS, topic)

    handlers = r.smembers(key)

    subscribers = len(handlers)
    watchers = None

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
        key = '{}.{}'.format(ORIGINS_WATCHERS, data['uuid'])

        handlers = r.smembers(key)
        watchers = len(handlers)

        if handlers:
            # TODO add to secondary queue for distributed work. For now
            # watchers handlers will be executed in sequence.
            for handler in handlers:
                if HTTP_RE.match(handler):
                    publish_web(handler, event, data)
                else:
                    publish_func(handler, event, data)

    return subscribers, watchers


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

    register_topic(topic)

    # List of in-code subscriptions for this event
    key = '{}.{}'.format(ORIGINS_SUBSCRIBERS, topic)

    r.sadd(key, handler)


def unsubscribe(topic, handler):
    r = get_redis_client()

    if not r:
        return

    key = '{}.{}'.format(ORIGINS_SUBSCRIBERS, topic)

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

    key = '{}.{}'.format(ORIGINS_WATCHERS, uuid)

    r.sadd(key, handler)


def unwatch(uuid, handler):
    r = get_redis_client()

    if not r:
        return

    key = '{}.{}'.format(ORIGINS_WATCHERS, uuid)

    r.srem(key, handler)


if __name__ == '__main__':
    r = get_redis_client()

    if not r:
        print('redis not configured')
        sys.exit(1)

    if '-q' in sys.argv:
        quiet = True
    else:
        quiet = False

    MAX_WORKERS = cpu_count()

    sys.stdout.write('[{}] Started Origins events daemon ({} workers):\n'
                     .format(datetime.now(), MAX_WORKERS))

    def run(topics):
        if not topics:
            return

        with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            # Submit jobs to executor getting back "futures". A dict is
            # used here to map the future to the topic name
            topic_futures = {
                ex.submit(dequeue, topic): topic
                for topic in topics
            }

            # Get results as they are completed
            for future in futures.as_completed(topic_futures):
                topic = topic_futures[future]
                now = datetime.now()

                try:
                    result = future.result()

                    if not quiet and result:
                        subscribers, watchers = result
                        watchers = '<na>' if watchers is None else watchers
                        sys.stdout.write('[{}] dequeued `{}` to {}/{}\n'
                                         .format(now, topic, subscribers,
                                                 watchers))
                except Exception:
                    sys.stderr.write('[{}] exception for `{}`:\n'
                                     .format(now, topic))
                    traceback.print_exc()

    while True:
        try:
            topics = r.smembers(ORIGINS_TOPICS)
            run(topics)
        except KeyboardInterrupt:
            break

        # Wait..
        time.sleep(1)
