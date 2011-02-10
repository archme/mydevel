#!/usr/bin/python2
## client.py

import os
import zmq
from zmq.eventloop import ioloop
#from zmq.eventloop import zmqstream
from  multiprocessing import Process

def client(cli_num):
  loop = ioloop.IOLoop.instance()
  print "Client %i (%i)" % (cli_num, os.getpid())

  context = zmq.Context()
  rpc_socket = context.socket(zmq.REQ)
  rpc_socket.connect(SRVQUEUE)
  
  cli_socket = context.socket(zmq.REQ)
  #cli_socket = context.socket(zmq.SUB)
  cli_identity = 'client' + str(cli_num)
  cli_socket.setsockopt(zmq.IDENTITY, cli_identity)
  cli_socket.connect(CLIQUEUE)

  msg = "test content from: " + str(cli_num)
  d = {"type":"msg", "body":msg, "id":cli_num}

  print "Client %i SENDING: %s" % (cli_num, d)
  rpc_socket.send_json(d)
  print "Client %i WAITING FOR REPLY ..." % (cli_num)

  def rep_handler(sock, events):
    reply = sock.recv_json()
    print "Client %i RECIEVED: %s" % (cli_num, reply)


  print "Client %i SENDING MULTIPRT: %s" % (cli_num, 'I am a client')
  #cli_socket.send('', zmq.SNDMORE)
  #cli_socket.send('I am a client')
  cli_socket.send_multipart([cli_identity, '', 'I am a client'])

  def rep_handler1(sock, events):
    print "Client %i WAITING FOR MULTIPART REPLY ..." % (cli_num)
    #delimit = cli_socket.recv()
    #msg = cli_socket.recv()
    reply = sock.recv_multipart()
    identity = reply[0]
    msg = reply[1]
    #name = msg[0]
    #msgtxt = msg[1]
    print "Client %i RECIEVED MULTIPART REPLY: %s" % (cli_num, msg)

  loop.add_handler(rpc_socket, rep_handler, zmq.POLLIN)
  loop.add_handler(cli_socket, rep_handler1, zmq.POLLIN)

  try:
    loop.start()
  except KeyboardInterrupt:
    print ""
    print "Client %i (%i) CANCELED !" % (cli_num, os.getpid())


if __name__ == '__main__':
  SRVQUEUE = "tcp://127.0.0.1:12345"
  CLIQUEUE = "tcp://127.0.0.1:23456"
  CLIENTS = 2
  
  clients_pool = range(CLIENTS)

  for cli_num in range(len(clients_pool)):
    Process(target=client, args=(cli_num,)).start()
