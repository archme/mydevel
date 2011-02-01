#!/usr/bin/python2

import sys

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.python import log

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

class AMQPProducer(object):
  def __init__(self, username='guest', password='guest', hostname='localhost', port=5672,
               vhost='/', specfile='./amqp0-8.xml', boxnr=19999):
    self.username = username
    self.password = password
    self.hostname = hostname
    self.port = port
    self.vhost = vhost
    self.specfile = specfile
    self.boxnr = boxnr
    
    self.spec = txamqp.spec.load(self.specfile)
    self.chan_name = 1
    self.exch_boxes_name = 'boxes_exchange_in'
    self.responsesExchange = 'client_response'

  def printError(self, failure):
    print >> sys.stderr, "Error:", failure
    reactor.stop()
    return

  def parseClientResponse(self, msg, chan):
    self.msg = msg
    self.chan = chan
    self.msgtxt = msg.content.body
    print self.msgtxt
    self.chan.basic_ack(delivery_tag=self.msg.delivery_tag)

  def connect(self):
    self.delegate = TwistedDelegate()
    self.cli = ClientCreator(reactor, AMQClient, delegate=self.delegate,
                                 vhost=self.vhost, spec=self.spec)
    print "Connecting to %s:%s..." % (self.hostname, self.port)
    self.conn = self.cli.connectTCP(self.hostname, self.port)
    self.conn.addCallback(self.connected)
    self.conn.addErrback(self.printError)

  @inlineCallbacks
  def connected(self, conn):
    self.conn = conn
    
    try:
      print "Authenticating user %s..." % self.username
      self.auth = yield self.conn.authenticate(self.username, self.password)
      print "Authenticated."

      print "Opening channel %s..." % self.boxnr
      chan = yield conn.channel(self.chan_name)
      yield chan.channel_open()
      print "Opened channel."
      
      print "Opening exchange %s..." % self.responsesExchange
      yield chan.exchange_declare(exchange=self.responsesExchange, type="topic")
      print "Opened exchange."
      
      print "Opening reply queue ..."
      reply = yield chan.queue_declare(exclusive=True, auto_delete=True)
      self.responseQueue = reply.queue
      yield chan.queue_bind(queue=self.responseQueue, exchange=self.responsesExchange, routing_key=self.responseQueue)
      reply = yield chan.basic_consume(queue=self.responseQueue)
      queue = yield conn.queue(reply.consumer_tag)
      d = queue.get()
      d.addCallback(self.parseClientResponse, chan)

      print "Opened queue."      
      
      msg = 'STOP'
      #msg = 'Message from Producer 01'
      print "Sending message: %s" % msg
      content = Content(body=msg)
      content["delivery mode"] = 2
      content["reply to"] = self.responseQueue
      chan.basic_publish(exchange=self.exch_boxes_name, content=content, routing_key='all.in')
      print "Sent message: %s" % msg

    except Exception, e:
      self.printError(e)
      return


def main(argv):
  producer = AMQPProducer()
  conn = producer.connect()

  reactor.run()
  sys.exit(0)

if __name__ == "__main__":
  try:
    main(sys.argv)
  except KeyboardInterrupt:
    print "User Interrupted"
