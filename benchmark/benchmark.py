import gevent.monkey
gevent.monkey.patch_all()

import argparse
import uuid

import logging
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)


import threading
import os
import sys
import redis
import redis.connection
from time import time
from operator import methodcaller

operations=['get', 'set']
DEFAULT_VALUE = 'uuid4'

parser = argparse.ArgumentParser(description='Raftis benchmark')
parser.add_argument('--host', default="localhost")
parser.add_argument('--port', type=int, default="6379")
parser.add_argument('--key', default='jay')
parser.add_argument('--value', default=DEFAULT_VALUE)
parser.add_argument('--operation', choices=operations, default='get')
parser.add_argument('--expected')
parser.add_argument('--requests', type=int, default=500)

args = parser.parse_args()

if args.operation == 'get' and args.expected == None:
    log.error('set requires expected value, add --expected')
    parser.print_usage()
    sys.exit(1)
 
r = redis.StrictRedis(host=args.host, port=args.port)
p = r.connection_pool
 
 
def main():
    maingreenlet = gevent.spawn(progress)
    log.info("{} greenlets connecting to raftis".format(args.requests))
    redisgreenlets = [gevent.spawn(ask_redis) for _ in xrange(args.requests)]
    # Wait until all greenlets have started and connected.
    gevent.sleep(1)

    stime = time()
    ask_redis()
    gevent.joinall(redisgreenlets)

    took= time() - stime
    print
    log.info("{:.2f} [req/s], took {:.2f} [s]".format(args.requests / took, took))
    maingreenlet.kill()
 
 
def ask_redis():
    raftis_command = [args.operation, args.key]
    expected = None

    if args.operation == 'set':
        expected = True
        if args.value == DEFAULT_VALUE:
            raftis_command.append(uuid.uuid4())
        else:
            raftis_command.append(args.value)

    if args.operation == 'get':
        expected = args.expected 

    response = methodcaller(*raftis_command)(r)
    if response != expected:
        log.error("expected: %s, got %s", expected, response)
 
 
def progress():
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
