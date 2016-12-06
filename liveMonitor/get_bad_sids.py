import BaseHTTPServer
import urlparse
from datetime import datetime

import redis

import http_server

redisClient = redis.StrictRedis(host='10.1.1.57', port=6379, db=0)
scores = set(['b1', 'g1', 'af'])


class HttpRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):
        query = dict(urlparse.parse_qsl(urlparse.urlparse(self.path).query))

        key = 'score'
        results = []

        if key in query:
            results = self.get_bad_sids(query[key])

        msg = '\n'.join(results)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(msg)

    def get_bad_sids(self, score):
        results = []
        if score not in scores:
            return results
        
        pattern = '%s:*' % score
        for key in redisClient.scan_iter(match=pattern):
            fields = key.split(':')
            ts = int(fields[1])
            sid = fields[2]
            value = fields[4]
            timestamp = datetime.utcfromtimestamp(ts)
            item = '%s:%s:%s' % (timestamp.strftime("%Y-%m-%d %H:%M:%S"), sid, value)
            results.append(item)
        return results


def main():
    http_server.setup_server(HttpRequestHandler, '0.0.0.0', 3001)


if __name__ == '__main__':
    main()
