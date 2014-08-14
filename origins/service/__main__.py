from __future__ import unicode_literals, absolute_import

import os
from origins.service.resources import app

HOST = os.environ.get('ORIGINS_HOST', 'localhost')
PORT = os.environ.get('ORIGINS_PORT', 5000)
DEBUG = os.environ.get('ORIGINS_DEBUG')

if DEBUG == '1':
    debug = True
else:
    debug = HOST in ('127.0.0.1', 'localhost')

app.run(debug=debug, threaded=True, host=HOST, port=int(PORT))
