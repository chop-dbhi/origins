from origins import conf
from origins.service.resources import app


app.run(threaded=True,
        debug=conf.options['debug'],
        host=conf.options['host'],
        port=conf.options['port'])
