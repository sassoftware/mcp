#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import os, sys
import testsuite
import testhelp
import random
import tempfile

from mcp import server, client, queue, config, mcp_error, response
import threading
import simplejson
from conary.conaryclient import cmdline
from conary import versions
from conary.lib import util
from conary.deps import deps

class SlaveBits:
    label = 'products.rpath.com@rpath:js'
    version = '3.0.0'
    revision = version + '-1-1'

    branch = '/' + label
    vbranch = branch + '/' + version
    rbranch = branch + '/' + revision

    labelObject = versions.Label(label)
    versionObject = versions.VersionFromString(rbranch)

    trove = ('group-jobslave', versionObject, deps.parseFlavor(''))
    NV = ('group-jobslave', versionObject)
    slaveDict = {version: trove[:2]}
    slaveListRV = (slaveDict, version)

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
        if dest.startswith('/queue/'):
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

    def getVersion(self, version=None):
        n = self.cfg.slaveTroveName
        v = SlaveBits.branch
        if version:
            v += '/' + version + '-1-1'
        else:
            v += '/' + SlaveBits.revision
        v = versions.VersionFromString(v)
        return n, v

class MCPTestMixin:
    def getMCPConfig(self):
        cfg = config.MCPConfig()
        cfg.basePath = self.mcpBasePath
        cfg.logPath = os.path.join(self.mcpBasePath, 'log')

        cfg.queueHost = 'dummyhost'
        cfg.queuePort = 12345
        cfg.namespace = 'test'

        cfg.slaveTroveName = 'group-jobslave'
        cfg.slaveSetVersion = '12345'
        cfg.slaveSetLabel = SlaveBits.label
        return cfg

    def setUp(self):
        self.mcpBasePath = tempfile.mkdtemp(prefix = 'mcp')
        os.mkdir(os.path.join(self.mcpBasePath, 'log'))
        os.mkdir(os.path.join(self.mcpBasePath, 'log', 'jobs'))
        self.mcpCfg = self.getMCPConfig()
        self.mcp = ThreadedMCP(self.mcpCfg)
        self.slaveId = 0

        self.q = queue.Queue(self.mcpCfg.queueHost, self.mcpCfg.queuePort,
                             'test', namespace = self.mcpCfg.namespace)
        self.q.timeOut = 0

        self.mcpClientCfg = client.MCPClientConfig()
        self.mcpClientCfg.namespace = 'test'
        self.mcpClient = client.MCPClient(self.mcpClientCfg)
        self.mcpClient.post.timeOut = 0
        self.buildCount = 0

        self.mcp.slaveInstallPath = set([SlaveBits.label])

        self.masterResponse = response.MCPResponse('master', self.mcpClientCfg)
        self.slaveResponse = response.MCPResponse('master:slave',
                                                  self.mcpClientCfg)
    def tearDown(self):
        self.mcp.running = False

        # Make sure logfiles get closed
        import logging
        log = logging.getLogger('')
        for handler in log.handlers:
            handler.close()
            log.removeHandler(handler)

        util.rmtree(self.mcpCfg.basePath, ignore_errors=True)
        util.rmtree(self.mcpBasePath, ignore_errors = True)


class MCPTest(testhelp.TestCase, MCPTestMixin):
    def setUp(self):
        testhelp.TestCase.setUp(self)
        MCPTestMixin.setUp(self)

        self.cfg = self.mcpCfg
        self.clientCfg = self.mcpClientCfg
        self.client = self.mcpClient

    def tearDown(self):
        testhelp.TestCase.tearDown(self)
        MCPTestMixin.tearDown(self)

    def getJsonBuild(self, jsversion = '2.0.2', arch = 'x86'):
        buildDict = {}
        buildDict['protocolVersion'] = 1
        buildDict['type'] = 'build'
        buildDict['UUID'] = 'test.rpath.local-build-%d-0' % self.buildCount
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

    def assertLogContent(self, content):
        f = open(os.path.join(self.cfg.logPath, 'mcp.log'))
        data = f.read()
        f.close()
        assert content in data
