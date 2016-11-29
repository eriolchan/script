import syslog
import SocketServer
import BaseHTTPServer

class ThreadingHttpServer(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer):
    pass


def setup_server(callback_handler, host, port):
    server = None
    try:
        server = ThreadingHttpServer((host, port), callback_handler)
        server.serve_forever()
    except KeyboardInterrupt:
        if server is not None:
            server.shutdown()
        syslog.syslog(syslog.LOG_WARNING, 'Server stop by keyboard interrupt')
        exit(1)
    except BaseException as e:
        if server is not None:
            server.shutdown()
        syslog.syslog(syslog.LOG_ERR, 'Server stop by exception: %s' % repr(e))
        exit(1)
