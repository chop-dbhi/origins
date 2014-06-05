from __future__ import unicode_literals

import logging

logger = logging.getLogger('origins.provenance')
logger.setLevel(logging.DEBUG)

debug = logger.debug
info = logger.info
warn = warning = logger.warning
error = logger.error
critical = logger.critical
fatal = logger.fatal
exception = logger.exception
