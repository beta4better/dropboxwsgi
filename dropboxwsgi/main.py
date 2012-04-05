#!/usr/bin/python
#
# This file is part of dropboxwsgi.
#
# Copyright (c) Dropbox, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

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
    class DictDerive(object, DictMixin): pass
except ImportError:
    # python 3
    from collections import MutableMapping as DictDerive

try:
    from gevent import pywsgi
except ImportError:
    pywsgi = None

from .dropboxwsgi import make_app, FileSystemCredStorage
from .caching import make_caching, FileSystemCache

logger = logging.getLogger(__name__)

def _start_server(app, host, port):
    if pywsgi:
        logger.info("Server is running; using gevent server")
        pywsgi.WSGIServer((host, port), app).serve_forever()
    else:
        logger.info("Server is running; using wsgiref server")
        make_server(host, port, app).serve_forever()

def console_output(str_, *args):
    print str_ % args

def usage(options, err='', argv=None):
    if argv is None:
        argv = sys.args

    if err:
        console_output(err)

    console_output("""Usage: %s %s [OPTION]
Run the dropboxwsgi HTTP server.
""", sys.executable, argv[0])

    def get_front_str(short, long_):
        if (short is not None and
            long_ is not None):
            return "-%s, --%s=ARG" % (short, long_)
        elif (short is None and
              long_ is not None):
            return "--%s=ARG" % long_
        elif (long_ is None and
              short_ is not None):
            return "-%s" % short

    def group_len(seq, len_):
        word_index = 0
        words = seq.split()
        toret = []
        while word_index < len(words):
            new_words = [words[word_index]]
            cur_len = len(new_words[-1])
            word_index += 1

            while word_index < len(words) and cur_len + len(new_words) - 1 < len_:
                new_words.append(words[word_index])
                cur_len += len(new_words[-1])
                word_index += 1

            toret.append(' '.join(new_words))

        return toret

    options_ = sorted(itertools.chain(options,
                                      [(None, None, 'h', 'help', None, None,
                                        'display this message and exit'),
                                       (None, None, 'c', 'config', None, None,
                                        'run with config ARG')]),
                      key=lambda x: (x[2] is None, x[2], x[3]))

    header_len = max(len(get_front_str(short, long_))
                     for (_, _, short, long_, _, _, doc) in options_)

    for (_, _, short, long_, _, _, doc) in options_:
        ops = get_front_str(short, long_)
        # TODO: use console width if exists
        min_seqs = group_len(doc, 80 - (header_len - 2))
        min_seqs = min_seqs or ['']
        console_output("%-*s  %s", header_len, ops, min_seqs[0])
        for elt in itertools.islice(min_seqs, 1, len(min_seqs)):
            console_output("%s  %s", " " * header_len, elt)

def config_from_options(options, argv):
    short_options = ''.join(itertools.chain(('%s:' % s for (_, _, s, _, _, _, _) in options
                                             if s is not None),
                                            ['h', 'c:']))
    long_options = ['%s=' % l for (_, _, _, l, _, _, _) in options
                    if l is not None]
    long_options.extend(['help', 'config='])

    try:
        opts, args = getopt.getopt(argv[1:], short_options, long_options)
    except getopt.GetoptError, err:
        # print help information and exit
        usage(options, str(err))
        raise SystemExit()

    config = dict((k, d) for (k, _, _, _, _, d, _) in options)
    def create_d(k, _a, _b, _c, conv, _d, _e):
        def d(arg): config[k] = conv(arg)
        return d

    dispatch = {}
    for a in options:
        (_, _, short, long_, _, _, _) = a
        d = create_d(*a)
        if short is not None:
            dispatch['-' + short] = d

        if long_ is not None:
            dispatch['--' + long_] = d

    def handle_help(a):
        raise Exception("")

    dispatch['-h'] = dispatch['--help'] = handle_help

    config_object = ConfigParser.SafeConfigParser()
    read_from = [os.path.expanduser("~/.dropboxwsgi/config")]
    def handle_config(a):
        read_from[0] = a

    dispatch['-c'] = dispatch['--config'] = handle_config

    for o, a in opts:
        try:
            dispatch[o](a)
        except Exception, e:
            usage(options, err=str(e), argv=argv)
            raise SystemExit()

    config_object.read(read_from)

    class TopConfigObject(DictDerive):
        def __init__(self, defaults, config_object, options):
            self.defaults = defaults
            self.config = config_object
            self.key_to_section = dict((k, (s, conv)) for (k, s, _, _, conv, _, _) in options)

        def __getitem__(self, k):
            (section, conv) = self.key_to_section[k]
            try:
                v = self.config.get(section, k)
            except ConfigParser.Error, e:
                # who knows doesn't exist for some reason
                return self.defaults[k]
            else:
                return conv(v)

        def __delitem__(self, k):
            raise NotImplementedError("Sorry")

        def __setitem__(self, k, v):
            raise NotImplementedError("Sorry")

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
                logging.WARNING, ('set minimum level when outputting log data. LEVEL can be one of '
                                  'debug, info, warning, error, critical, exception')),

               ('consumer_key', 'Credentials', None, 'consumer-key', identity, None,
                'consumer key to use when accessing the Dropbox API'),
               ('consumer_secret', 'Credentials', None, 'consumer-secret', identity, None,
                'consumer secret to use when accessing the Dropbox API'),
               ('access_type', 'Credentials', None, 'access-type', access_type_from_string, None,
                ('access type to use when accessing the Dropbox API. can be one of '
                 'dropbox or app_folder')),

               ('http_root', 'Server', None, 'http-root', identity, None,
                ('http root to use when redirecting and creating absolute links, '
                 'e.g. "http://www.example.com"')),
               ('listen', 'Server', None, 'listen', address_from_string, ('', 80),
                'address for server to listen on, e.g. "0.0.0.0:80"'),
               ('enable_local_caching', 'Server', None, 'enable-local-caching', bool_from_string,
                True, 'true if you want to cache data from the Dropbox API on this server, false otherwise'),
               ('validate_wsgi', 'Server', None, 'validate-wsgi', bool_from_string, False,
                ('true if you want to apply the wsgi.validator.validate '
                 'decorator to this WSGI app, false otherwise')),
               ('allow_directory_listing', 'Server', None, 'allow-directory-listing',
                bool_from_string, True,
                'true if you want to allow directory listings, false otherwise'),

               ('cache_dir', 'Storage', None, 'cache-dir', identity,
                os.path.expanduser("~/.dropboxwsgi/cache"),
                'path to use when caching data from the Dropbox API locally'),
               ('app_dir', 'Storage', None, 'app-dir', identity,
                os.path.expanduser("~/.dropboxwsgi"),
                'path to use for storing internal app data, like access credentials')]

    try:
        config = config_from_options(options, argv)
    except SystemExit, e:
        return 2

    # generate config object, backends to options then file
    logging.basicConfig(level=config['log_level'])

    if config['http_root'] is None:
        usage(options, err="error: Must specify http-root!", argv=argv)
        return 3

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
