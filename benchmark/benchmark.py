import gevent.monkey
gevent.monkey.patch_all()

import argparse

import logging
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)


import threading
import os
import sys
import redis
import redis.connection
from time import time
from operator import methodcaller

parser = argparse.ArgumentParser(description='Raftis benchmark')
parser.add_argument('--host', default="localhost")
parser.add_argument('--port', type=int, default="6379")
parser.add_argument('--key', default='jay')
parser.add_argument('--operation', default='get')
parser.add_argument('--expected', default='5')
parser.add_argument('--requests', type=int, default=500)
aargs = parser.parse_args()

 
r = redis.StrictRedis(host=aargs.host, port=aargs.port)
p = r.connection_pool
 
 
def main():
    crongreenlet = gevent.spawn(cron)
    log.info("{} greenlets connecting to raftis".format(aargs.requests))
    redisgreenlets = [gevent.spawn(ask_redis) for _ in xrange(aargs.requests)]
    # Wait until all greenlets have started and connected.
    gevent.sleep(1)

    stime = time()
    gevent.joinall(redisgreenlets)

    took= time() - stime
    print
    log.info("{:.2f} [req/s], took {:.2f} [s]".format(aargs.requests / took, took))
    crongreenlet.kill()
 
 
def ask_redis():
    assert methodcaller(aargs.operation, aargs.key)(r) is aargs.expected 
 
 
def cron():
    while True:
        print('.'),
        sys.stdout.flush()
        gevent.sleep(0.2)
 
 
if __name__ == "__main__":
    main()


#import redis
#import random
#import threading
#from time import time
#from multiprocessing import Process
#from operator import methodcaller
#
#
#
#
#th = []
#
#
#def bang():
#    r = redis.Redis(host='raftis-0-slc-7231.lvs01.dev.ebayc3.com')
#    s = time()
#    last = 0
#    for i in range(1, 1000):
#        r.get('jay')
#        c = time()
#        if c - s >= 1.0:
#             s=c
#             print("{} [req/s]".format(i - last))
#             last = i


#for i in range(1):
#    at = Process(target=bang)
#    th.append(at)
#    at.start()
#
#
#map(methodcaller('join'), th)
