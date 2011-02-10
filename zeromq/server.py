#!/usr/bin/python2
## server.py

import os
import time
import zmq
from zmq.eventloop import ioloop
from  multiprocessing import Process


def worker(wrk_num):
  loop = ioloop.IOLoop.instance()

  print "Worker %i (%i)" % (wrk_num, os.getpid())

  context = zmq.Context()
  rpc_socket = context.socket(zmq.REP)
  rpc_socket.connect(RPCQUEUE)

  pub_socket = context.socket(zmq.REP)
  pub_socket.setsockopt(zmq.IDENTITY, 'worker' + str(wrk_num))
  pub_socket.connect(CLIQUEUE)

  def rep_handler(sock, events):
    request = sock.recv_json()
    print "Worker %i RECIEVED: %s" % (wrk_num, request)

    print "Worker %i PROCESSING ..." % (wrk_num)
    time.sleep(5)

    print "Worker %i REPLYING: %s" % (wrk_num, request)
    sock.send_json(request)

  def rep_handler1(sock, events):
    rep = sock.recv_multipart()
    identity = rep[0]
    delimit = rep[1]
    msgtxt = rep[2]
    print "Worker %i RECIEVED MULTIPART from: %s" % (wrk_num, identity)
    reply = "Hello " + identity
    print "Worker %i REPLYING MULTIPART to: %s" % (wrk_num, identity)
    sock.send_multipart([identity, reply])
    #sock.send(identity, zmq.SNDMORE)
    #sock.send('', zmq.SNDMORE)
    #sock.send('Hello' + 'identity')

  def pinger():
    print "pinger called."
    pub_socket.send_multipart(['client1', '', 'test to Client1'])
    asw = pub_socket.recv_multipart()
    print asw

  loop.add_handler(rpc_socket, rep_handler, zmq.POLLIN)
  loop.add_handler(pub_socket, rep_handler1, zmq.POLLIN)
  timer = ioloop.PeriodicCallback(pinger, 5000, loop)
  timer.start()
  try:
    loop.start()
  except KeyboardInterrupt:
    print ""
    print "Worker %i (%i) CANCELED !" % (wrk_num, os.getpid())


if __name__ == "__main__":
  RPCQUEUE = "tcp://127.0.0.1:54321"
  CLIQUEUE = "tcp://127.0.0.1:65432"
  WORKERS = 2
  
  worker_pool = range(WORKERS)

  for wrk_num in range(len(worker_pool)):
      Process(target=worker, args=(wrk_num,)).start()
