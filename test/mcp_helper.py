#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import os
import testsuite
import random
import tempfile

from mcp import server, client, queue, config, mcp_error, response
import threading
import simplejson
from conary.conaryclient import cmdline
from conary import versions
from conary.lib import util

class DummyConnection(object):
    def __init__(self, *args, **kwargs):
        self.sent = []
        self.listeners = []
        self.subscriptions = []
        self.unsubscriptions = []
        self.acks = []

    def send(self, dest, message):
        self.sent.append((dest, message))

    def receive(self, message):
        for listener in self.listeners:
            listener.receive(message)

    def subscribe(self, dest, ack = 'auto'):
        assert ack == 'client', 'Queue will not be able to refuse a message'
        self.subscriptions.insert(0, dest)

    def unsubscribe(self, dest):
        self.unsubscriptions.insert(0, dest)

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
        message = 'message-id: dummy-message\n\n\n' + message
        self.receive(message)

    def disconnect(self):
        pass

class DummyQueue(object):
    type = 'queue'

    def __init__(self, host, port, dest, namespace = 'test', timeOut = 600,
                 queueLimit = None, autoSubscribe = True):
        self.connectionName = '/' + '/'.join((self.type, 'test', dest))
        self.incoming = []
        self.outgoing = []
        self.messageCount = 0

    def send(self, message):
        assert type(message) in (str, unicode), \
            "Can't put non-strings in a queue"
        message = 'message-id: message-%d\n\n\n' % self.messageCount + message
        self.messageCount += 1
        self.outgoing.insert(0, message)

    def read(self):
        return self.incoming and self.incoming.pop() or None

    def disconnect(self):
        pass

class DummyMultiplexedQueue(DummyQueue):
    def __init__(self, host, port, dest = [], namespace = 'test',
                 timeOut = 600, queueLimit = None, autoSubscribe = True):
        self.incoming = []
        self.outgoing = []
        self.messageCount = 0

    def send(self, dest, message):
        assert type(message) in (str, unicode), \
            "Can't put non-strings in a queue"
        message = 'message-id: message-%d\n\n\n' % self.messageCount + message
        self.messageCount += 1
        self.outgoing.insert(0, (dest, message))

    def addDest(self, dest):
        pass

class DummyTopic(DummyQueue):
    type = 'topic'

class DummyMultiplexedTopic(DummyMultiplexedQueue):
    type = 'topic'

class ThreadedMCP(server.MCPServer, threading.Thread):
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self)
        server.MCPServer.__init__(self, *args, **kwargs)

    def getVersion(self, version):
        if '-' not in version:
            version += '-1-1'
        nvf = cmdline.parseTroveSpec('%s=/%s/%s' % \
                        (self.cfg.slaveTroveName, self.cfg.slaveTroveLabel,
                         version))
        return nvf[0], versions.VersionFromString(nvf[1]), \
            (nvf[2] and nvf[2] or '')

class MCPTest(testsuite.TestCase):
    def setUp(self):
        testsuite.TestCase.setUp(self)
        self.basePath = tempfile.mkdtemp(prefix = 'mcp')
        os.mkdir(os.path.join(self.basePath, 'log'))
        self.cfg = self.getMCPConfig()
        self.mcp = ThreadedMCP(self.cfg)
        self.slaveId = 0

        self.q = queue.Queue(self.cfg.queueHost, self.cfg.queuePort,
                             'test', namespace = self.cfg.namespace)
        self.q.timeOut = 0

        self.clientCfg = client.MCPClientConfig()
        self.clientCfg.namespace = 'test'
        self.client = client.MCPClient(self.clientCfg)
        self.client.response.timeOut = 0
        self.buildCount = 0

        self.masterResponse = response.MCPResponse('master', self.clientCfg)
        self.slaveResponse = response.MCPResponse('master:slave',
                                                  self.clientCfg)

    def tearDown(self):
        self.mcp.running = False
        util.rmtree(self.cfg.basePath)
        testsuite.TestCase.tearDown(self)

    def getMCPConfig(self):
        cfg = config.MCPConfig()
        cfg.basePath = self.basePath
        cfg.logPath = os.path.join(cfg.basePath, 'log')

        cfg.queueHost = 'dummyhost'
        cfg.queuePort = 12345
        cfg.namespace = 'test'

        cfg.slaveTroveName = 'group-core'
        cfg.slaveTrpveLabel = 'conary.rpath.com@rpl:1'
        return cfg

    def getJsonBuild(self, jsversion = '2.0.2', arch = 'x86'):
        buildDict = {}
        buildDict['serialVersion'] = 1
        buildDict['type'] = 'build'
        buildDict['UUID'] = 'test.rpath.local:build-%d' % self.buildCount
        self.buildCount += 1
        buildDict['data'] = {}
        buildDict['data']['jsversion'] = jsversion
        if arch == 'x86':
            buildDict['troveFlavor'] = "1#x86"
        elif arch == 'x86_64':
            buildDict['troveFlavor'] = "1#x86:~i486:~i586:~i686:~sse2|1#x86_64"
        return simplejson.dumps(buildDict)

    def checkValue(self, d, key, value):
        self.failIf(key not in d,
                    "missing %s attribute" % key)
        self.failIf(d[key] != value,
                    "expected %s of %s but got %s" % (key, value, d[key]))

