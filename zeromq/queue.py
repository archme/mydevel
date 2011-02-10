#!/usr/bin/python2
## queue.py

import time
import zmq
from zmq import devices

def main():
  queue = devices.basedevice.ProcessDevice(zmq.QUEUE, zmq.XREP, zmq.XREQ)
  queue.bind_in('tcp://*:12345')
  queue.bind_out('tcp://*:54321')
  queue.start()

  queue1 = devices.basedevice.ProcessDevice(zmq.QUEUE, zmq.XREP, zmq.XREQ)
  queue1.bind_in('tcp://*:23456')
  queue1.bind_out('tcp://*:65432')
  queue1.start()

  #queue1 = devices.basedevice.ProcessDevice(zmq.QUEUE, zmq.PUB, zmq.SUB)
  #queue1.bind_in('tcp://*:23456')
  #queue1.bind_out('tcp://*:65432')
  #queue1.start()


  try:
    while True:
      time.sleep(1)
      pass
  except (KeyboardInterrupt):
      print ""
      print "CANCELED"


if __name__ == '__main__':
  main()
