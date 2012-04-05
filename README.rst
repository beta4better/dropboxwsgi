dropbox_wsgi
============

``dropbox_wsgi`` is a Python_ package that provides a WSGI_ application that
implements an HTTP_ interface into the `Dropbox API`_. This package
also includes a server application, also called ``dropbox_wsgi``, that runs
the WSGI application. The HTTP caching headers (ETag, Last-Modified) are
fully supported.

This is useful in situations where you'd like to serve out a web site
out of your Dropbox, either to the world or in a private network.
Compare this to the current solution of linking the Dropbox desktop
client on your server and serving out of your Dropbox folder.

This package also contains a caching middleware that will cache data
from the Dropbox API onto the local disk (or whatever storage
implementation you provide) to eliminate redundant data transfer
between your server and Dropbox.

By the way, this package also supports Python 3. Yay!

Installation
------------

Installation is easy and fun::

  $ python setup.py install

Server Application Usage
------------------------

``dropbox_wsgi`` is both a server application and a library. Let's try
using it on the command line first::

  $ cat <<EOF > config.ini
  [Credentials]
  consumer_key = <your_dropbox_api_key>
  consumer_secret = <your_dropbox_api_secret>
  access_type = app_folder

  [Server]
  http_root = http://localhost:8080
  listen = 8080
  EOF
  $ dropbox_wsgi -c config.ini -l info
  Server is running; using wsgiref server"

Pretty simple. Now point your browser to ``http://localhost:8080/``. If
you want to run this in production I recommend using the gevent_ WSGI
server and using nginx_ as frontend proxy.

Library Usage
-------------

WSGI applications, like ``dropbox_wsgi``, have the benefit of being
able to run in a variety of server environments. `App Engine`_ and
Heroku_ come to mind but running it on your own VPS works too. Let's
try it using the reference WSGI implementation included with Python::

  #!/usr/bin/python

  from wsgiref.simple_server import make_server
  from dropbox_wsgi.dropbox_wsgi import make_app, MemoryCredStorage
  
  config = dict(http_root="http://localhost:8080",
                consumer_key="<your dropbox api key",
                consumer_secret="<your dropbox api secret>",
                access_type"dropbox or app_folder")

  app = make_app(config, MemoryCredStorage())

  make_server("", 8080, app).serve_forever()

The vanilla WSGI application provides the standard HTTP caching headers
(both ETag and Last-Modified) but otherwise doesn't litter the HTTP
header space. This makes it so you can layer as much middleware as you
want between the application and the server.

Extensibility
-------------

If I haven't stressed it already enough quite yet, let me try once more.
There are dozens of middleware packages available for WSGI and even more
proxy servers that speak HTTP. Extending ``dropbox_wsgi`` is simply a
matter of hooking things together. Do you want a caching Dropbox-backed
server with HTTP auth? Squid_ + nginx + ``dropbox_wsgi`` solves your
problem. The possibilities are endless!

Copyright Stuff
---------------

Copyright (c) Dropbox, Inc.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

.. _Python: http://www.python.org/
.. _WSGI: http://http://www.python.org/dev/peps/pep-3333/
.. _HTTP: http://www.w3.org/Protocols/rfc2616/rfc2616.html
.. _Dropbox API: https://www.dropbox.com/developers
.. _gevent: http://www.gevent.org/
.. _nginx: http://nginx.org/
.. _App Engine: https://developers.google.com/appengine/docs/python/tools/webapp/running
.. _Heroku: https://devcenter.heroku.com/articles/python
.. _Squid: http://www.squid-cache.org/
