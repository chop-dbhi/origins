from origins import config
from origins.service import app


app.run(threaded=True,
        debug=config.options['debug'],
        host=config.options['host'],
        port=config.options['port'])
