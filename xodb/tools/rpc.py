import sys

ADDRESS = 'localhost'

PORT = 2730

URI = 'http://%s:%s/' % (ADDRESS, PORT)


def make_application(database, address=ADDRESS, port=PORT):
    from xodb import JSONDatabase
    import wsgi_jsonrpc

    database = JSONDatabase(database)
    application = wsgi_jsonrpc.WSGIJSONRPCApplication(instance=database)
    return application


def run_server(database, address=ADDRESS, port=PORT,
               reloader=None, threaded=True, processes=1,
               errors_fatal=False, echo_sql=False):
    from werkzeug.serving import run_simple

    app = make_application(database, address, port)

    print >> sys.stderr, " * Listening on %s:%s." % (address, port)
    run_simple(address, port, app,
               use_reloader=reloader,
               reloader_interval=1,
               threaded=threaded,
               processes=processes,
               passthrough_errors=errors_fatal,
               )

