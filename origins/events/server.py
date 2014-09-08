import sys
import time
import json
import traceback
from datetime import datetime
from concurrent import futures
from multiprocessing import cpu_count
from origins.events import client, logger
from origins.events import utils


MAX_WORKERS = cpu_count()


def dequeue(topic):
    "Dequeues published events for the given topic."
    key = '{}.{}'.format(utils.EVENTS_KEY, topic)

    value = client.rpop(key)

    # Nothing in the events queue
    if value is None:
        return

    logger.debug('dequeuing "{}"'.format(key))

    value = json.loads(value)

    event = value['event']
    data = value['data']

    topic = event['topic']

    # Subscriptions for this event
    key = '{}.{}'.format(utils.SUBSCRIBERS_KEY, topic)

    handlers = client.smembers(key)

    subscribers = len(handlers)
    watchers = None

    if handlers:
        # TODO add to secondary queue for distributed work. For now
        # subscriber handlers will be executed in sequence.
        for handler in handlers:
            if utils.HTTP_RE.match(handler):
                utils.publish_web(handler, event, data)
                logger.debug('POSTed to URL "{}" for "{}"'
                             .format(handler, key))
            else:
                utils.publish_func(handler, event, data)
                logger.debug('called function "{}" for "{}"'
                             .format(handler, key))

    # Event contains object UUID. Check for watchers
    if data and 'uuid' in data:
        key = '{}.{}'.format(utils.WATCHERS_KEY, data['uuid'])

        handlers = client.smembers(key)
        watchers = len(handlers)

        if handlers:
            # TODO add to secondary queue for distributed work. For now
            # watchers handlers will be executed in sequence.
            for handler in handlers:
                if utils.HTTP_RE.match(handler):
                    utils.publish_web(handler, event, data)
                    logger.debug('POSTed to URL "{}" for "{}"'
                                 .format(handler, key))
                else:
                    utils.publish_func(handler, event, data)
                    logger.debug('called function "{}" for "{}"'
                                 .format(handler, key))

    return subscribers, watchers


def run(topics, quiet=True):
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
                future.result()
            except Exception:
                sys.stderr.write('[{}] exception for `{}`:\n'
                                 .format(now, topic))
                traceback.print_exc()


def serve():
    sys.stdout.write('[{}] Started Origins events daemon ({} workers):\n'
                     .format(datetime.now(), MAX_WORKERS))

    try:
        while True:
            topics = client.smembers(utils.TOPICS_KEY)
            run(topics)
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass

    sys.stdout.write('\r[{}] Shutting down...\n'
                     .format(datetime.now()))
