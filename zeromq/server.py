#!/usr/bin/python2

import os
import time
import zmq
from zmq.eventloop import ioloop, zmqstream

SRVQUEUE = "tcp://127.0.0.1:54321"

loop = ioloop.IOLoop.instance()

print 'Server', os.getpid()

context = zmq.Context()
socket = context.socket(zmq.REP)
#socket.bind(SRVQUEUE)
socket.connect(SRVQUEUE)

#stream = zmqstream.ZMQStream(socket, loop)

#def rep_handler(request):
def rep_handler(sock, events):
    request = sock.recv_json()
    print "RECIEVED: %s" % request

    print "PROCESSING ..."
    time.sleep(5)

    print "REPLY: %s" % request
    sock.send_json(request)
    #socket.send(request[0])

loop.add_handler(socket, rep_handler, zmq.POLLIN)
#stream.on_recv(rep_handler)

try:
  loop.start()
except KeyboardInterrupt:
  print ""
  print "CANCELED"