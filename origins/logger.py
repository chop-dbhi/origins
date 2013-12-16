"Pre-defined logger for Origins."
from __future__ import unicode_literals, absolute_import
import sys
import logging

logger = logging.getLogger('origins')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

debug = logger.debug
info = logger.info
warning = logger.warning
error = logger.error
critical = logger.critical
fatal = logger.fatal
exception = logger.exception
