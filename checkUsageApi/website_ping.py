#!/usr/bin/python

import socket
import threading
import time
import urllib2


def website_ping():
    ping()
    
    global timer
    timer = threading.Timer(10, website_ping)
    timer.start()


def ping():
  url = 'http://www.agora.io/cn/'
  ctime = time.ctime()
  print 'ping at', ctime

  start = time.time()
  try:
    res = urllib2.urlopen(url, timeout=15)
    print 'OK', res.getcode()
    res.close()
  except urllib2.HTTPError as e:
    print 'Fail', ctime, e.code, e.reason
  except urllib2.URLError, e:
    print 'Fail', ctime, e.reason
  except socket.timeout, e:
    print 'Timeout'

  end = time.time()
  print 'latency=%d' % (end - start) 


def main():
    timer = threading.Timer(10, website_ping)
    timer.start()


if __name__ == '__main__':
    main()
