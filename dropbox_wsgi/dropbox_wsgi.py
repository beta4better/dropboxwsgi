import calendar
import errno
import logging
import os
import pprint
import tempfile
import time
import traceback

try:
    import json
except Exception:
    import simplejson as json

from cgi import parse_qs
from wsgiref.simple_server import make_server
from wsgiref.validate import validator

import dropbox

from dropbox.rest import ErrorResponse

logger = logging.getLogger(__name__)

HTTP_PRECONDITION_FAILED = 412
HTTP_NOT_MODIFIED = 304
HTTP_OK = 200

def tz_offset(tz_string):
    factor = 1 if tz_string[0] == '+' else -1
    hours = 3600 * int(tz_string[1:3])
    minutes = 60 * int(tz_string[3:5])
    return factor * (hours + minutes)

def dropbox_date_to_posix(date_string):
    fmt_date, tz = date_string.rsplit(' ', 1)
    ts = calendar.timegm(time.strptime(fmt_date, "%a, %d %b %Y %H:%M:%S"))
    return ts + tz_offset(tz)

def posix_to_http_date(ts=None):
    if ts is None:
        ts = time.time()
    HTTP_DATE_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
    return time.strftime(HTTP_DATE_FORMAT, time.gmtime(ts))

def http_date_to_posix(date_string):
    # parse date string in three different formats
    # 1) Sun, 06 Nov 1994 08:49:37 GMT  ; RFC 822, updated by RFC 1123
    # 2) Sunday, 06-Nov-94 08:49:37 GMT ; RFC 850, obsoleted by RFC 1036
    # 3) Sun Nov  6 08:49:37 1994       ; ANSI C's asctime() format
    for fmt in ["%a, %d %b %Y %H:%M:%S GMT",
                "%A, %d-%b-%y %H:%M:%S GMT",
                "%a %b %d %H:%M:%S %Y"]:
        try:
            _tt = time.strptime(date_string, fmt)
        except ValueError:
            continue
        return calendar.timegm(_tt)
    else:
        raise ValueError("Date could not be parsed")

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
        start_response('304 NOT MODIFIED', [])
        return []

    def precondition_failed_response(environ, start_response):
        start_response('412 PRECONDITION FAILED', [('Content-type', 'text/plain')])
        return ['Precondition Failed!']

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

        # get etag
        MATCH_ANY = object()
        def get_match(key_name):
            try:
                if_none_match = environ[key_name]
            except KeyError:
                return None
            else:
                if if_none_match.strip() == "*":
                    return MATCH_ANY
                else:
                    return [a.strip() for a in if_none_match.split(',')]

        if_match = get_match('HTTP_IF_MATCH')
        if_none_match = get_match('HTTP_IF_NONE_MATCH')

        # generate the kw args for metadata()
        # based on the passed in etag
        if (if_none_match is not None and
            if_none_match is not MATCH_ANY and
            len(if_none_match) == 1 and
            if_none_match[0].startswith('"d')):
            kw = {'hash' : if_none_match[0][2:-1]}
        else:
            kw = {}

        allow_directory_listing = config.get('allow_directory_listing', True)

        # get the metadata for this call
        try:
            md = client.metadata(path, list=allow_directory_listing, **kw)
        except Exception, e:
            if isinstance(e, ErrorResponse):
                if e.status == 304:
                    return not_modified_response(environ, start_response)
                elif e.status == 404:
                    pass
                else:
                    logger.exception("API Error")
            else:
                logger.exception("API Error")

            is_deleted = True
        else:
            is_deleted = md.get('is_deleted')

        if is_deleted:
            # if the file is deleted just cancel early
            return not_found_response(environ, start_response)

        if md['is_dir'] and not allow_directory_listing:
            # if we're not allowing directory listings
            # just exit early
            start_response('403 FORBIDDEN', [('Content-type', 'text/plain')])
            return ['Forbidden']

        if md['is_dir']:
            current_etag = '"d%s"' % (md['hash'].encode('utf8'),)
            current_modified_date = None
            def toret1(environ, start_response):
                start_response('200 OK', [('Content-type', 'text/html'),
                                          ('ETag', current_etag)])
                yield '''<!DOCTYPE html>
<html>
<head>
<title>Index of %(path)s</title>
<style type="text/css">
a, a:active {text-decoration: none; color: blue;}
a:visited {color: #48468F;}
a:hover, a:focus {text-decoration: underline; color: red;}
body {background-color: #F5F5F5;}
table {margin-left: 12px;}
h1 { font-size: -1;}
th, td { font: 90%% monospace; text-align: left;}
th { font-weight: bold; padding-right: 14px; padding-bottom: 3px;}
td {padding-right: 14px;}
td.s, th.s {text-align: right;}
div.list { background-color: white; border-top: 1px solid #646464; border-bottom: 1px solid #646464; padding-top: 10px; padding-bottom: 14px;}
div.foot { font: 90%% monospace; color: #787878; padding-top: 4px;}
</style>
</head>
<body>
<h1>Index of %(path)s</h1>
<div class="list">
<table summary="Directory Listing" cellpadding="0" cellspacing="0">
<thead>
<tr>
<th class="n">Name</th>
<th class="m">Last Modified</th>
<th class="s">Size</th>
<th class="t">Type</th>
</tr>
</thead>
<tbody>
''' % dict(path=md['path'])

                if md['path'] != u'/':
                    yield '<tr>\n'
                    yield '<td class="n"><a href="../">Parent Directory/</a></td>\n'
                    yield '<td class="m"></td>\n'
                    yield '<td class="s">-&nbsp;&nbsp;</td>\n'
                    yield '<td class="t">Directory</td>\n'
                    yield '</tr>\n'

                for entry in md['contents']:
                    name = entry['path'].rsplit('/', 1)[1]
                    yield '<tr>\n'
                    yield '<td class="n"><a href="%s">%s%s</a></td>\n' % (entry['path'], name,
                                                                        "/" if entry['is_dir'] else "")
                    yield '<td class="m">%s</td>\n' % time.strftime("%Y-%b-%d %H:%M:%S", time.gmtime(dropbox_date_to_posix(entry['modified'])))
                    yield '<td class="s">%s</td>\n' % ('- &nbsp;' if entry['is_dir'] else entry['size'])
                    yield '<td class="t">%s</td>\n' % ('Directory' if entry['is_dir'] else entry['mime_type'])
                    yield '</tr>\n'

                yield '''</tbody>
</table>
</div>
<div class="foot">%(server_software)s</div>
</body>
</html>''' % dict(server_software=environ.get('SERVER_SOFTWARE', 'Dropbox HTTP/1.0'))
            toret = toret1
        else:
            current_etag = '"_%s"' % md['rev'].encode('utf8')
            current_modified_date = dropbox_date_to_posix(md['modified'].encode('utf8'))
            def toret2(environ, start_response):
                dropbox_date = md['modified'].encode('utf8')
                last_modified_date = posix_to_http_date(dropbox_date_to_posix(dropbox_date))
                start_response('200 OK', [('Content-Type', md['mime_type'].encode('utf8')),
                                          ('Cache-Control', 'public, no-cache'),
                                          ('Content-Length', str(md['bytes'])),
                                          ('Date', posix_to_http_date()),
                                          ('ETag', current_etag),
                                          ('Last-Modified', last_modified_date)])

                res = client.get_file(path, rev=md['rev'])
                def gen():
                    try:
                        while True:
                            ret = res.read(block_size)
                            if not ret:
                                break
                            yield ret
                    finally:
                        res.close()

                return gen()
            toret = toret2

        # it's nice to have this as a separate function
        def http_cache_logic(current_etag, current_modified_date, if_match, if_none_match, last_modified_since):
            logger.debug("current_etag: %r", current_etag)
            logger.debug("current_modified_date: %r", current_modified_date)
            logger.debug("if_match: %r", if_match)
            logger.debug("if_none_match: %r", if_none_match)
            logger.debug("last_modified_since: %r", last_modified_since)

            if if_match is not None and not (if_match is MATCH_ANY or any(e[1:] == md['rev'] for e in if_match)):
                logger.debug("precondition failed")
                return HTTP_PRECONDITION_FAILED

            if ((if_none_match is not None and (if_none_match is MATCH_ANY or any(e == current_etag for e in if_none_match)) and
                 (last_modified_since is None or current_modified_date is None or current_modified_date <= last_modified_since)) or
                # this logic sucks, this is the case where if_none_match is not specified
                (if_none_match is None and last_modified_since is not None and current_modified_date is not None and current_modified_date <= last_modified_since)):
                logger.debug("not modified")
                return HTTP_NOT_MODIFIED

            logger.debug("return ok")
            return HTTP_OK

        try:
            last_modified_since = environ['HTTP_IF_MODIFIED_SINCE']
        except KeyError:
            last_modified_since = None
        else:
            last_modified_since = http_date_to_posix(last_modified_since)

        return_code = http_cache_logic(current_etag, current_modified_date,
                                       if_match, if_none_match, last_modified_since)
        if return_code == HTTP_PRECONDITION_FAILED:
            return precondition_failed_response(environ, start_response)
        elif return_code == HTTP_NOT_MODIFIED:
            return not_modified_response(environ, start_response)
        else:
            return toret(environ, start_response)

    return app

if __name__ == "__main__":
    config = dict(consumer_key='iodc7pv1hlolg5a',
                  consumer_secret='bqynhr0h1ivucm5',
                  access_type='app_folder',
                  allow_directory_listing=True,
                  http_root='http://localhost:8080',
                  app_dir=os.path.expanduser('~/.dropboxhttp'))

    logging.basicConfig(level=logging.DEBUG)
    impl = AppImpl(config['app_dir'])
    make_server('', 8080, validator(make_app(config, impl))).serve_forever()
