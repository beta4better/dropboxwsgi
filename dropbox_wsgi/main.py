from __future__ import absolute_import

import logging
import os
import sys

try:
    import json
except Exception:
    import simplejson as json

from wsgiref.simple_server import make_server
from wsgiref.validate import validator

try:
    from gevent import pywsgi
except Exception:
    pywsgi = None

from .dropbox_wsgi import make_app
from .caching import make_caching, FileSystemCache

logger = logging.getLogger(__name__)

def _start_server(app, host, port):
    if pywsgi:
        logger.debug("using gevent!")
        pywsgi.WSGIServer((host, port), app).serve_forever()
    else:
        logger.debug("using wsgiref!")
        make_server(host, port, app).serve_forever()

def main(args=None):
    if args is None:
        args = sys.argv

    logging.basicConfig(level=logging.DEBUG)

    config = dict(consumer_key='iodc7pv1hlolg5a',
                  consumer_secret='bqynhr0h1ivucm5',
                  access_type='app_folder',
                  allow_directory_listing=True,
                  http_root='http://localhost:8080',
                  app_dir=os.path.expanduser('~/.dropboxhttp'))
    impl = AppImpl(config['app_dir'])
    host = '0.0.0.0'
    port = 8080

    app = make_app(config, impl)

    app = make_caching(FileSystemCache(config['app_dir']))(app)

    app = validator(app)

    _start_server(app, host, port)

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
