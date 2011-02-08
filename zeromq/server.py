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

  pub_socket = context.socket(zmq.PUB)
  pub_socket.connect(CLIQUEUE)

  def rep_handler(sock, events):
    request = sock.recv_json()
    print "Worker %i RECIEVED: %s" % (wrk_num, request)

    print "Worker %i PROCESSING ..." % (wrk_num)
    time.sleep(5)

    msg = "test publish from: " + str(wrk_num)
    d = {"type":"msg", "body":msg}
    print "Worker %i PUBLISHING: %s ..." % (wrk_num, d)

    print "Worker %i REPLYING: %s" % (wrk_num, request)
    sock.send_json(request)

  loop.add_handler(rpc_socket, rep_handler, zmq.POLLIN)
  try:
    loop.start()
  except KeyboardInterrupt:
    print ""
    print "Worker %i (%i) CANCELED !" % (wrk_num, os.getpid())


if __name__ == "__main__":
  RPCQUEUE = "tcp://127.0.0.1:54321"
  CLIQUEUE = "tcp://127.0.0.1:65432"
  WORKERS = 10
  
  worker_pool = range(WORKERS)

  for wrk_num in range(len(worker_pool)):
      Process(target=worker, args=(wrk_num,)).start()
