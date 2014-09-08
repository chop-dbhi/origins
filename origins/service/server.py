from .app import app
from origins import config


def serve(host=None, port=None, debug=None):
    host = host or config.options['host']
    port = port or config.options['port']
    debug = debug or config.options['debug']

    app.run(threaded=True,
            debug=debug,
            host=host,
            port=port)
