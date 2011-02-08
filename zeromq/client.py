#!/usr/bin/python2
## client.py

import os
import zmq
from zmq.eventloop import ioloop
#from zmq.eventloop import zmqstream
from  multiprocessing import Process

def client(cli_num):
  print "Client %i (%i)" % (cli_num, os.getpid())

  context = zmq.Context()
  socket = context.socket(zmq.REQ)
  socket.connect(SRVQUEUE)

  msg = "test content from: " + str(cli_num)
  d = {"type":"msg", "body":msg, "id":cli_num}

  print "Client %i SENDING: %s" % (cli_num, d)
  socket.send_json(d)
  print "Client %i WAITING FOR REPLY ..." % (cli_num)

  reply = socket.recv_json()
  print "Client %i RECIEVED: %s" % (cli_num, reply)


if __name__ == '__main__':
  SRVQUEUE = "tcp://127.0.0.1:12345"
  CLIENTS = 10
  
  clients_pool = range(CLIENTS)

  for cli_num in range(len(clients_pool)):
    Process(target=client, args=(cli_num,)).start()
