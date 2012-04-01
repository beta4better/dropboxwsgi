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

class AppImpl(object):
    def __init__(self, app_dir):
        # TODO: raise error if app_dir is not ASCII
        app_dir = app_dir
        self.tmp_dir = os.path.join(app_dir, 'tmp')
        self.cache_dir = os.path.join(app_dir, 'data')
        self.md_cache_dir = os.path.join(app_dir, 'metadata')
        self.request_token_path = os.path.join(app_dir, 'request_token')
        self.access_token_path = os.path.join(app_dir, 'access_token')

        # if these fail, let the exception raise
        # TODO: blow these away if they are files
        # TODO: check permissions
        for p in (self.cache_dir, self.md_cache_dir, self.tmp_dir):
            if not os.path.isdir(p):
                os.makedirs(p)

    def read_access_token(self):
        # TODO: check validity of data stored in file
        # and blow away if invalid
        with open(self.access_token_path, 'rb') as f:
            return json.load(f)

    def write_access_token(self, key, secret):
        with open(self.access_token_path, 'wb') as f:
            json.dump((key, secret), f)

    def read_cached_metadata(self, path):
        with open(os.path.join(self.md_cache_dir, *path[1:].split('/')), 'rb') as f:
            return json.load(f)

    def drop_cached_data(self, path):
        os_path_pieces = path[1:].split('/')
        for parent in (self.md_cache_dir, self.cache_dir):
            toremove = os.path.join(parent, *os_path_pieces)
            try:
                os.unlink(toremove)
            except OSError, e:
                if e.errno != errno.ENOENT:
                    logger.exception("Couldn't remove: %r", toremove)

    def read_cached_data(self, path):
        with open(os.path.join(self.md_cache_dir, *path[1:].split('/')), 'rb') as f:
            return (open(os.path.join(self.cache_dir, *path[1:].split('/')), 'rb'),
                    json.load(f))

    def write_cached_data(self, path, md):
        os_path_pieces = path[1:].split('/')
        s1 = self
        class NoOp(object):
            def __init__(self):
                self.written = 0
                fd, self.path = tempfile.mkstemp(dir=s1.tmp_dir)
                self.f = os.fdopen(fd, 'wb')
            def write(self, data):
                self.f.write(data)
                self.written += len(data)
            def close(self):
                unlink = True
                try:
                    self.f.close()
                    if self.written == md.get('bytes'):
                        # mv the file and write the metadata
                        # TODO: this should be atomic, dir move?
                        os.rename(self.path, os.path.join(s1.cache_dir, *os_path_pieces))
                        unlink = False
                        with open(os.path.join(s1.md_cache_dir, *os_path_pieces), 'wb') as f:
                            json.dump(md, f)
                finally:
                    if unlink:
                        os.unlink(self.path)
            def __enter__(self):
                return self
            def __exit__(self, *n, **kw):
                self.close()
        return NoOp()

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
