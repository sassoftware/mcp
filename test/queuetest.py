#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()
import simplejson

import os
import time
import threading
from mcp import queue
import stomp
import mcp_helper

class DummyConnection(object):
    def __init__(self, *args, **kwargs):
        self.sent = []
        self.listeners = []
        self.subscriptions = []
        self.unsubscriptions = []
        self.acks = []
        self.messageCount = 0

    def send(self, dest, message):
        self.sent.append((dest, message))

    def subscribe(self, dest, ack = 'auto'):
        assert ack == 'client', 'Queue will not be able to refuse a message'
        self.subscriptions.append(dest)

    def unsubscribe(self, dest):
        self.unsubscriptions.append(dest)

    def addlistener(self, listener):
        if listener not in self.listeners:
            self.listeners.append(listener)

    def dellistener(self, listener):
        if listener in self.listeners:
            self.listeners.remove(listener)

    def start(self):
        pass

    def ack(self, messageId):
        self.acks.append(messageId)

    def insertMessage(self, message):
        message = 'message-id: message-%d\n\n\n' % self.messageCount + message
        self.messageCount += 1
        for listener in self.listeners:
            listener.receive(message)

    def disconnect(self):
        pass

class QueueTest(mcp_helper.MCPTest):
    def setUp(self):
        self._savedConnection = stomp.Connection
        stomp.Connection = DummyConnection
        mcp_helper.MCPTest.setUp(self)

    def tearDown(self):
        mcp_helper.MCPTest.tearDown(self)
        stomp.Connection = self._savedConnection

    def testSend(self):
        self.q.send('test')
        assert self.q.connection.sent == [('/queue/test/test', 'test')]

    def testRead(self):
        assert self.q.read() == None
        self.q.connection.insertMessage('test')
        assert self.q.read() == 'test'

    def testReadTimeout(self):
        startTime = time.time()
        self.q.timeOut = 0
        self.q.read()
        delta = time.time() - startTime
        assert delta < 1
        self.q.timeOut = delta
        startTime = time.time()
        self.q.read()
        assert (time.time() - startTime) > delta

    def testRecv(self):
        assert self.q.read() == None
        self.q.receive('message-id: message-test\n\n\ntest')
        assert self.q.read() == 'test'
        assert self.q.connection.acks == ['message-test']
        assert self.q.read() == None

    def testSetLimit(self):
        q = queue.Queue('dummyhost', 12345, 'test', namespace = 'test')

        assert q.queueLimit is None
        assert q.limitedQueue is False

        q.queueLimit = 0
        assert q.limitedQueue == True
        assert q.queueLimit == 0

        q.queueLimit = None
        assert q.queueLimit is None
        assert q.limitedQueue is False

    def testLimitedQueue(self):
        q = queue.Queue('dummyhost', 12345, 'test', namespace = 'test')

        assert q.queueLimit is None
        q.incrementLimit()
        assert q.queueLimit is None

        q.queueLimit = 0
        q.incrementLimit()

        assert q.queueLimit == 1

    def testReadLimit(self):
        self.q.queueLimit = 1
        self.q.connection.subscriptions = []
        self.q.connection.unsubscriptions = []
        self.q.connection.insertMessage('test')

        assert self.q.connection.unsubscriptions == ['/queue/test/test']
        assert self.q.connection.subscriptions == []

        self.q.connection.unsubscriptions = []
        assert self.q.read() == 'test'

        assert self.q.connection.subscriptions == []
        assert self.q.connection.unsubscriptions == []

        self.q.incrementLimit()
        assert self.q.connection.subscriptions == ['/queue/test/test']

    def testReadRejection(self):
        self.q.queueLimit = 1

        assert self.q.connection.acks == []

        self.q.connection.insertMessage('test')
        assert self.q.connection.acks == ['message-0']
        self.q.connection.insertMessage('test')
        assert self.q.connection.acks == ['message-0']
        self.q.incrementLimit()
        self.q.connection.insertMessage('test')
        assert self.q.connection.acks == ['message-0', 'message-2']

    def testReadAckLimits(self):
        self.q.queueLength = 1
        self.q.connection.subscriptions = []
        self.q.connection.unsubscriptions = []
        self.q.connection.acks = []
        self.q.connection.insertMessage('test')

        assert self.q.inbound == ['test']
        assert self.q.connection.acks == ['message-0']

    def testMQRead(self):
        q = queue.MultiplexedQueue('dummyhost', 12345, namespace = 'test')
        q.connection.insertMessage('test')
        assert q.read() == 'test'

    def testMultiplexedSubscriptions(self):
        q = queue.MultiplexedQueue('dummyhost', 12345, namespace = 'test')
        q.addDest('test1')
        q.addDest('test2')

        assert q.connection.subscriptions == \
            ['/queue/test/test1', '/queue/test/test2']

        q.delDest('test2')

        assert q.connection.unsubscriptions == ['/queue/test/test2']

    def testMultiplexedReadLimit(self):
        q = queue.MultiplexedQueue('dummyhost', 12345, namespace = 'test')
        q.addDest('test1')
        q.addDest('test2')
        q.queueLimit = 1
        q.connection.subscriptions = []
        q.connection.unsubscriptions = []
        q.connection.insertMessage('test')

        assert q.connection.unsubscriptions == \
            ['/queue/test/test1', '/queue/test/test2']
        assert q.connection.subscriptions == []
        assert q.connection.acks == ['message-0']

        q.connection.unsubscriptions = []
        assert q.read() == 'test'

        assert q.connection.subscriptions == []
        assert q.connection.unsubscriptions == []

        q.incrementLimit()
        assert q.connection.subscriptions == \
            ['/queue/test/test1', '/queue/test/test2']

    def testMultiplexedReadAcks(self):
        q = queue.MultiplexedQueue('dummyhost', 12345, namespace = 'test')
        q.addDest('test1')
        q.addDest('test2')
        q.queueLimit = 1
        q.connection.subscriptions = []
        q.connection.unsubscriptions = []
        q.connection.insertMessage('test')
        assert q.connection.acks == ['message-0']
        q.connection.insertMessage('test')
        assert q.connection.acks == ['message-0']
        q.incrementLimit()
        q.connection.insertMessage('test')
        assert q.connection.acks == ['message-0', 'message-2']

    def testMultiplexedSend(self):
        q = queue.MultiplexedQueue('dummyhost', 12345, namespace = 'test')
        q.send('testdest', 'test')
        q.connection.sent == [('/queue/test/testdest', 'test')]

    def testMultiplexedRecv(self):
        q = queue.MultiplexedQueue('dummyhost', 12345, namespace = 'test')
        q.receive('message-id: test-message\n\n\ntest')
        assert q.inbound == ['test']
        assert q.connection.acks == ['test-message']


if __name__ == "__main__":
    testsuite.main()
