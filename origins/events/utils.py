import re
import time
import json
import uuid
import inspect
import requests
from importlib import import_module
from ._redis import client as redis


HTTP_RE = re.compile(r'^https?://', re.I)

TOPICS = 'origins.topics'
SUBSCRIBERS = 'origins.subscribers'
WATCHERS = 'origins.watchers'
EVENTS = 'origins.events'


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
    if not redis:
        return

    redis.flushdb()


def register_topic(topic):
    if not redis:
        return

    redis.sadd(TOPICS, topic)
