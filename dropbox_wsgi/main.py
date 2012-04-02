from __future__ import absolute_import

import ConfigParser
import getopt
import itertools
import logging
import os
import sys
import traceback

try:
    import json
except Exception:
    import simplejson as json

from wsgiref.simple_server import make_server
from wsgiref.validate import validator

try:
    from UserDict import DictMixin
except ImportError:
    # python 3
    from collections import MutableMapping as DictMixin

try:
    from gevent import pywsgi
except ImportError:
    pywsgi = None

from .dropbox_wsgi import make_app, FileSystemCredStorage
from .caching import make_caching, FileSystemCache

logger = logging.getLogger(__name__)

def _start_server(app, host, port):
    if pywsgi:
        logger.debug("using gevent!")
        pywsgi.WSGIServer((host, port), app).serve_forever()
    else:
        logger.debug("using wsgiref!")
        make_server(host, port, app).serve_forever()

def console_output(str_):
    print str_

def config_from_options(options, argv):
    short_options = ''.join(itertools.chain(('%s:' % s for (_, _, s, _, _, _) in options
                                             if s is not None),
                                            ['h', 'c:']))
    long_options = ['%s=' % l for (_, _, _, l, _, _) in options
                    if l is not None]
    long_options.extend(['help', 'config='])

    def usage(err=''):
        if err:
            console_output(err)

        console_output("""Usage: %(executable)s %(progname)s [OPTION]
Run the dropbox_wsgi HTTP server.""" % dict(executable=sys.executable,
                                            progname=argv[0]))

    try:
        opts, args = getopt.getopt(argv[1:], short_options, long_options)
    except getopt.GetoptError, err:
        # print help information and exit
        usage(str(err))
        raise SystemExit()

    config = dict((k, d) for (k, _, _, _, _, d) in options)
    def create_d(k, section, short, long_, conv, _):
        def d(arg): config[k] = conv(arg)
        return d

    dispatch = {}
    for a in options:
        (_, _, short, long_, _, _) = a
        d = create_d(*a)
        if short is not None:
            dispatch['-' + short] = d

        if long_ is not None:
            dispatch['--' + long_] = d

    def handle_help(a):
        raise Exception("")

    dispatch['-h'] = dispatch['--help'] = handle_help

    config_object = ConfigParser.SafeConfigParser()
    read_from = [os.path.expanduser("~/.dropboxhttp/config")]
    def handle_config(a):
        read_from[0] = a

    dispatch['-c'] = dispatch['--config'] = handle_config

    for o, a in opts:
        try:
            dispatch[o](a)
        except Exception, e:
            usage(str(e))
            raise SystemExit()

    config_object.read(read_from)

    class TopConfigObject(object, DictMixin):
        def __init__(self, defaults, config_object, options):
            self.defaults = defaults
            self.config = config_object
            self.key_to_section = dict((k, (s, conv)) for (k, s, _, _, conv, _) in options)

        def __getitem__(self, k):
            (section, conv) = self.key_to_section[k]
            try:
                v = self.config.get(section, k)
            except ConfigParser.Error, e:
                # who knows doesn't exist for some reason
                return self.defaults[k]
            else:
                return conv(v)

        def __iter__(self):
            return itertools.chain(self.defaults,
                                   (o
                                    for sec in self.config.sections()
                                    for o in self.config.options(sec)
                                    if o not in self.defaults))

        def __len__(self):
            return sum(1 for _ in self)

        def keys(self):
            return list(self)

    return TopConfigObject(config, config_object, options)

def main(argv=None):
    if argv is None:
        argv = sys.argv

    def log_level_from_string(a):
        log_level_name = a.upper()
        if log_level_name not in ["DEBUG", "INFO", "WARNING",
                                  "ERROR", "CRITICAL", "EXCEPTION"]:
            raise Exception("not a log level: %r" % a)
        return getattr(logging, log_level_name)

    def identity(a): return a

    def access_type_from_string(a):
        if a not in ['app_folder', 'dropbox']:
            raise Exception("not an access type: %r" % a)
            return 2
        return a

    def bool_from_string(a):
        al = a.lower()
        if al == 'true':
            return True
        elif al == 'false':
            return False
        else:
            raise Exception("not a boolean: %r" % a)

    def address_from_string(a):
        splitted = a.split(':', 1)
        if len(splitted) == 2:
            host = splitted[0]
            port = int(splitted[1])
        else:
            try:
                port = int(a)
                host = ''
            except ValueError:
                port = 80
                host = a
        return (host, port)

    # [(top_level_dict_key, config_section_name, short_option, long_option, from_string, default)]
    options = [('log_level', 'Debugging', 'l', 'log-level', log_level_from_string,
                logging.WARNING),

               ('consumer_key', 'Credentials', None, 'consumer-key', identity, None),
               ('consumer_secret', 'Credentials', None, 'consumer-secret', identity, None),
               ('access_type', 'Credentials', None, 'access-type', access_type_from_string, None),

               ('http_root', 'Server', None, 'http-root', identity, None),
               ('listen', 'Server', None, 'listen', address_from_string, ('', 80)),
               ('enable_local_caching', 'Server', None, 'enable-local-caching', bool_from_string,
                True),
               ('validate_wsgi', 'Server', None, 'validate-wsgi', bool_from_string, False),
               ('allow_directory_listing', 'Server', None, 'allow-directory-listing',
                bool_from_string, True),

               ('cache_dir', 'Storage', None, 'cache-dir', identity,
                os.path.expanduser("~/.dropboxhttp/cache")),
               ('app_dir', 'Storage', None, 'app-dir', identity,
                os.path.expanduser("~/.dropboxhttp"))]

    try:
        config = config_from_options(options, argv)
    except SystemExit, e:
        return 2

    # generate config object, backends to options then file
    logging.basicConfig(level=config['log_level'])

    app = make_app(config, FileSystemCredStorage(config['app_dir']))

    if config['enable_local_caching']:
        app = make_caching(FileSystemCache(config['cache_dir']))(app)

    if config['validate_wsgi']:
        app = validator(app)

    (host, port) = config['listen']
    _start_server(app, host, port)

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
