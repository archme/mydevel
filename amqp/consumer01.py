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

from twisted.enterprise import adbapi


class DataBase(object):
  def __init__(self, dbdriver='sqlite3', dbfile='./mydb'):
    self.dbdriver = dbdriver
    self.dbfile = dbfile

  def connect(self):
    self.dbpool = adbapi.ConnectionPool(self.dbdriver, database=self.dbfile, check_same_thread=False)
    return self.dbpool

  def query(self, dbpool, querystring):
    self.dbpool = dbpool
    self.querystring = querystring
    
    self.d = self.dbpool.runQuery(self.querystring)
    self.result = self.d.addCallback(self._query_finished)
    return self.result

  def _query_finished(self, result):
    self.result = result
    print self.result


class AMQPConsumer(object):
  def __init__(self, username='guest', password='guest', hostname='localhost', port=5672,
               vhost='/', specfile='./amqp0-8.xml', boxnr=9999):
    self.username = username
    self.password = password
    self.hostname = hostname
    self.port = port
    self.vhost = vhost
    self.specfile = specfile
    self.boxnr = boxnr
    
    self.spec = txamqp.spec.load(self.specfile)
    self.exch_in_name = 'boxes_exchange_in'
    self.exch_out_name = 'boxes_exchange_out'
    self.chan_name = 1
    self.queue_in_name = 'box_queue_' + str(self.boxnr) + '_in'


  def printError(self, failure):
    print >> sys.stderr, "Error:", failure
    reactor.stop()
    return

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

      print "Opening channel %s..." % self.chan_name
      chan = yield conn.channel(self.chan_name)
      yield chan.channel_open()
      print "Opened channel."
      
      print "Opening queue %s..." % self.queue_in_name
      yield chan.queue_declare(queue=self.queue_in_name, exclusive=False)
      print "Opened queue."

      print "Opening exchange %s..." % self.exch_in_name
      yield chan.exchange_declare(exchange=self.exch_in_name, type="topic")
      print "Opened exchange."
      
      print "Binding exchange %s to %s with routing key %s..." % (self.exch_in_name, self.queue_in_name, str(self.boxnr) + '.in')
      yield chan.queue_bind(queue=self.queue_in_name, exchange=self.exch_in_name, routing_key=str(self.boxnr) + '.in')
      print "Binded exchange."

      print "Binding exchange %s to %s with routing key %s..." % (self.exch_in_name, self.queue_in_name, 'all.in')
      yield chan.queue_bind(queue=self.queue_in_name, exchange=self.exch_in_name, routing_key='all.in')
      print "Binded exchange."

      yield chan.basic_consume(queue=self.queue_in_name, no_ack=True, consumer_tag='box' + str(self.boxnr))

      queue = yield conn.queue('box' + str(self.boxnr))

      while True:
	  msg = yield queue.get()
	  print 'Received: "' + msg.content.body + ', Return to: ' + msg.content["reply to"] + '" from channel #' + str(chan.id)
	  if msg.content.body == "STOP":
	    break
	  txt = 'Consumer 01 got your msg'
	  content = Content(body=txt)
	  chan.basic_publish(exchange="client_response", content=content, routing_key=msg.content["reply to"])

      #yield chan.basic_cancel('box' + str(self.boxnr))
      yield chan.channel_close()
      reactor.stop()

    except Exception, e:
      self.printError(e)
      return


def main(argv):
  nr = int(argv[1])
  print nr
  #db = DataBase()
  #dbcon = db.connect()

  consumer = AMQPConsumer(boxnr=nr)
  conn = consumer.connect()
  
  #for i in range(1, 3):
    #db.query(dbcon, 'SELECT * FROM users')

  reactor.run()
  sys.exit(0)

if __name__ == "__main__":
  try:
    main(sys.argv)
  except KeyboardInterrupt:
    print "User Interrupted"
