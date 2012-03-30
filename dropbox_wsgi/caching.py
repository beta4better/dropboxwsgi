import functools

from wsgiref.util import FileWrapper

def make_caching(impl):
    def wrapper(app):
        @functools.wraps(app)
        def new_app(environ, start_response):
            # if the client is already sending up
            # the caching headers then use that
            if ('HTTP_IF_MODIFIED_SINCE' in environ or
                'HTTP_IF_NONE_MATCH' in environ):
                return app(environ, start_response)

            path = environ['PATH_INFO']

            try:
                etag = impl.read_cached_etag(path)
            except Exception:
                logger.exception("Couldn't read cached data")
            else:
                environ['HTTP_IF_NONE_MATCH'] = etag

            writer = [None]
            def make_writer(etag):
                yield
                f = impl.write_cached_data(path, etag)
                try:
                    while True:
                        data = yield
                        if not data:
                            break
                        f.write(data)
                finally:
                    f.close()

            top_res = []
            def my_start_response(code, headers):
                top_res[:] = [code, headers]
                if code.startswith('304'):
                    # we don't care about the 304 response
                    # we'll send out the file data after
                    def noop(_): return
                    return noop
                else:
                    etag = None
                    if code.startswith('200'):
                        # save new data with etag if it exists
                        for h, d in headers:
                            if h.lower() == 'etag':
                                # save etag
                                etag = d
                                break

                    if etag is not None:
                        # they are going to pass data into this thing,
                        # save it!!
                        top_writer = start_response(code, headers)
                        writer[0] = make_writer(etag)
                        writer[0].next()
                        @functools.wraps(top_writer)
                        def new_writer(data):
                            writer[0].send(data)
                            return top_writer(data)
                        return new_writer
                    else:
                        return start_response(code, headers)

            res = app(environ, my_start_response)
            if top_res[0].startswith('304'):
                # send out saved data
                fwrapper = environ.get('wsgi.file_wrapper', FileWrapper)
                block_size = 16 * 1024
                toret = fwrapper(impl.read_cached_data(path), block_size)
            elif writer[0] is not None:
                # handle the rest of data for saving
                def better_res():
                    for d in res:
                        writer[0].send(d)
                        yield d
                    writer[0].send('')
                toret = better_res()
            else:
                toret = res

            return toret
        return new_app
    return wrapper
