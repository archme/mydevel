#!/usr/bin/python2
## client.py

import sys
import os
import zmq
from zmq.eventloop import ioloop, zmqstream

MYID = sys.argv[1]
SRVQUEUE = "tcp://127.0.0.1:12345"

def main():
  context = zmq.Context()
  socket = context.socket(zmq.REQ)
  socket.connect(SRVQUEUE)

  msg = "%s" % "test content"
  d = {"type":"msg", "body":msg, "id":MYID}

  print "SENDING: %s" % d
  socket.send_json(d)
  #socket.send("xxxxxxxxxxx")
  print "WAITING FOR REPLY ..."

  reply = socket.recv_json()
  #reply = socket.recv()
  print "RECIEVED: %s" % reply


if __name__ == '__main__':
  print 'Client', os.getpid()
  main()
