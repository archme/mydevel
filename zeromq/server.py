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
  socket = context.socket(zmq.REP)
  socket.connect(SRVQUEUE)

  def rep_handler(sock, events):
    request = sock.recv_json()
    print "Worker %i RECIEVED: %s" % (wrk_num, request)

    print "Worker %i PROCESSING ..." % (wrk_num)
    time.sleep(5)

    print "Worker %i REPLYING: %s" % (wrk_num, request)
    sock.send_json(request)

  loop.add_handler(socket, rep_handler, zmq.POLLIN)
  try:
    loop.start()
  except KeyboardInterrupt:
    print ""
    print "Worker %i (%i) CANCELED !" % (wrk_num, os.getpid())


if __name__ == "__main__":
  SRVQUEUE = "tcp://127.0.0.1:54321"
  WORKERS = 10
  
  worker_pool = range(WORKERS)

  for wrk_num in range(len(worker_pool)):
      Process(target=worker, args=(wrk_num,)).start()
