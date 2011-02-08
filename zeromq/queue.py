#!/usr/bin/python2
## queue.py

import time
import zmq
from zmq import devices

def main():
  #queue = devices.basedevice.ProcessDevice(zmq.QUEUE, zmq.REP, zmq.REQ)
  queue = devices.basedevice.ProcessDevice(zmq.QUEUE, zmq.XREP, zmq.XREQ)
  queue.bind_in('tcp://*:12345')
  #queue.connect_out('tcp://*:54321')
  queue.bind_out('tcp://*:54321')
  queue.start()

  try:
    while True:
      print "here"
      time.sleep(1)
  except (KeyboardInterrupt):
      print ""
      print "DONE"


if __name__ == '__main__':
  main()
