import errno
import logging
import os
import pprint
import sqlite3
import tempfile
import traceback

try:
    import json
except Exception:
    import simplejson as json

from cgi import parse_qs
from wsgiref.simple_server import make_server

import dropbox

from dropbox.rest import ErrorResponse

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

def make_app(config, impl):
    http_root = config['http_root']
    finish_link_path = '/finish_link'
    block_size = 16 * 1024

    sess = dropbox.session.DropboxSession(config['consumer_key'],
                                          config['consumer_secret'],
                                          config['access_type'])

    # get token
    try:
        at = impl.read_access_token()
    except Exception:
        # TODO check exception type
        traceback.print_exc()
    else:
        sess.set_token(*at)

    client = dropbox.client.DropboxClient(sess)

    def link_app(environ, start_response):
        # this is the pingback
        if environ['PATH_INFO'] == finish_link_path:
            query_args = parse_qs(environ['QUERY_STRING'])

            oauth_token = query_args['oauth_token'][0]
            if oauth_token != sess.request_token.key:
                raise Exception("Non-matching request token")

            try:
                at = sess.obtain_access_token()
            except Exception:
                # request token was bad, link again
                sess.request_token = None
            else:
                impl.write_access_token(at.key, at.secret)
                start_response('200 OK', [('Content-type', 'text/plain')])
                return ['Server is now Linked! Browse at will']

        # check if we have a request_token lying around already
        if not sess.request_token:
            sess.obtain_request_token()

        auth_url = sess.build_authorize_url(sess.request_token,
                                            http_root + finish_link_path)
        start_response('302 FOUND', [('Content-type', 'text/plain'),
                                     ('Location', auth_url)])
        return ['Redirecting...']

    def not_found_response(environ, start_response):
        start_response('404 NOT FOUND', [('Content-type', 'text/plain')])
        return ['Not Found!']

    def not_modified_response(environ, start_response):
        start_response('304 NOT MODIFIED', [('Content-type', 'text/plain')])
        return ['Not Modified!']

    def app(environ, start_response):
        # checked if we are linked yet
        if not sess.is_linked():
            return link_app(environ, start_response)

        # turn path into unicode
        for enc in ['utf8', 'latin1']:
            try:
                path = environ['PATH_INFO'].decode('utf8')
            except UnicodeDecodeError:
                pass
            else:
                break
        else:
            return not_found_response(environ, start_response)

        # TODO: take E-Tag from header
        try:
            etag = environ['HTTP_IF_NONE_MATCH']
        except KeyError:
            kwargs = {}
            etag = None
        else:
            kwargs = ({'hash' : etag[1:]}
                      if etag[0] == 'd' else
                      {})

        try:
            md = client.metadata(path, **kwargs)
        except Exception, e:
            if isinstance(e, ErrorResponse):
                if e.status == 304:
                    return not_modified_response(environ, start_response)
                elif e.status != 404:
                    logger.exception("API Error")
            else:
                logger.exception("API Error")
            is_deleted = True
        else:
            is_deleted = md.get('is_deleted')

        if is_deleted:
            return not_found_response(environ, start_response)
        elif md['is_dir']:
            # it's a directory, we have to re-retrieve
            # but we should check if the file is the same before
            # sending out directory contents
            #md = client.metadata(path)
            start_response('200 OK', [('Content-type', 'text/plain'),
                                      ('ETag', 'd' + md['hash'])])
            return [pprint.pformat(md)]
        elif etag is not None and md.get('rev') == etag[1:]:
            return not_modified_response(environ, start_response)
        else:
            res = client.get_file(path, rev=md.get('rev'))
            def gen():
                try:
                    while True:
                        ret = res.read(block_size)
                        if not ret:
                            break
                        yield ret
                finally:
                    res.close()

            start_response('200 OK', [('Content-type', md['mime_type'].encode('utf8')),
                                      ("Cache-Control", "public, max-age=3600"),
                                      ('Content-Length', str(md['bytes'])),
                                      ('ETag', '"f' + md['rev'].encode('utf8') + '"')])
            return gen()
    return app

def make_caching(impl):
    def wrapper(app):
        @functools.wraps(app)
        def new_app(environ, start_response):
            # check if md matches cached version
            if md2.get('rev') == md.get('rev'):
                try:
                    f, md2 = impl.read_cached_data(path)
                except Exception:
                    class Closeable(object):
                        def close(self):
                            pass
                    f = Closeable()
                    md2 = {}

                if md2.get('rev') != md.get('rev'):
                    # the stored file has changed in the interim
                    # close the file and fallthrough
                    f.close()
                else:
                    try:
                        fwrapper = environ['wsgi.file_wrapper']
                    except KeyError:
                        class ReadableWrapper(object):
                            def __init__(self, f, block_size):
                                self.f = f
                                self.block_size = block_size
                            def close(self):
                                self.f.close()
                            def __iter__(self):
                                return iter(functools.partial(self.f.read, self.block_size), '')

                        fwrapper = ReadableWrapper
                    return fwrapper(f, block_size)

            def wrap(gen):
                try:
                    f = impl.write_cached_data(path, md)
                except Exception:
                    # couldn't write, just stream out the data
                    for data in gen:
                        yield data
                else:
                    try:
                        for data in gen:
                            f.write(data)
                            yield data
                    finally:
                        f.close()

            return wrap(gen())
        return new_app
    return wrapper

if __name__ == "__main__":
    config = dict(consumer_key='iodc7pv1hlolg5a',
                  consumer_secret='bqynhr0h1ivucm5',
                  access_type='app_folder',
                  http_root='http://localhost:8080',
                  app_dir='/home/rian/.dropboxhttp')

    logging.basicConfig(level=logging.DEBUG)
    impl = AppImpl(config['app_dir'])
    make_server('', 8080, make_app(config, impl)).serve_forever()
