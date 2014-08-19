import sys
import json


def stream_handler(event, data):
    sys.stdout.write(json.dumps({
        'event': event,
        'data': data,
    }, indent=4))

    sys.stdout.write('\n')
