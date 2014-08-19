#!/usr/bin/env python3

import sys
import time
import traceback
from datetime import datetime
from concurrent import futures
from multiprocessing import cpu_count
from origins import events
from origins.events import utils
from origins.events._redis import client as redis

if '-q' in sys.argv:
    quiet = True
else:
    quiet = False

if not redis:
    print('redis not configured')
    sys.exit(1)


MAX_WORKERS = cpu_count()


def run(topics):
    if not topics:
        return

    with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        # Submit jobs to executor getting back "futures". A dict is
        # used here to map the future to the topic name
        topic_futures = {
            ex.submit(events.dequeue, topic): topic
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


sys.stdout.write('[{}] Started Origins events daemon ({} workers):\n'
                 .format(datetime.now(), MAX_WORKERS))

try:
    while True:
        topics = redis.smembers(utils.TOPICS)
        run(topics)
        time.sleep(0.1)
except KeyboardInterrupt:
    pass


sys.stdout.write('\r[{}] Shutting down...\n'
                 .format(datetime.now()))
