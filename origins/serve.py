#!/usr/bin/env python
from __future__ import unicode_literals, absolute_import


if __name__ == '__main__':
    import os
    from origins.service import app

    HOST = os.environ.get('ORIGINS_HOST', '127.0.0.1:5000')
    DEBUG = os.environ.get('ORIGINS_DEBUG')

    if ':' in HOST:
        host, port = HOST.split(':')
        port = int(port)
    else:
        host = HOST
        port = None

    if DEBUG == '1':
        debug = True
    else:
        debug = host in ('127.0.0.1', 'localhost')

    app.run(debug=debug, threaded=True, host=host, port=port)
