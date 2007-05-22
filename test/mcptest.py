#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()
import simplejson
import StringIO
import time

import os, sys
import threading
from mcp import queue
from mcp import constants
from conary import conaryclient
from conary.conaryclient import cmdline
from conary import versions
from conary.errors import TroveNotFound

from mcp import server, mcp_error, jobstatus, slavestatus
import mcp_helper

class DummyQueue(object):
    def __init__(self, host, port, dest, namespace = 'test', timeOut = 600,
                 queueLimit = None, autoSubscribe = True):
        self.incoming = []
        self.outgoing = []

    def send(self, message):
        assert type(message) in (str, unicode), \
            "Can't put non-strings in a queue"
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

    def send(self, dest, message):
        DummyQueue.send(self, message)

    def addDest(self, dest):
        pass

class McpTest(mcp_helper.MCPTest):
    def setUp(self):
        self._savedQueue = queue.Queue
        self._savedTopic = queue.Topic
        self._savedMultiplexedQueue = queue.MultiplexedQueue
        self._savedMultiplexedTopic = queue.MultiplexedTopic
        queue.Queue = DummyQueue
        queue.Topic = DummyQueue
        queue.MultiplexedQueue = DummyMultiplexedQueue
        queue.MultiplexedTopic = DummyMultiplexedQueue
        mcp_helper.MCPTest.setUp(self)

    def tearDown(self):
        mcp_helper.MCPTest.tearDown(self)
        queue.Queue = self._savedQueue
        queue.Topic = self._savedTopic
        queue.MultiplexedQueue = self._savedMultiplexedQueue
        queue.MultiplexedTopic = self._savedMultiplexedTopic

    def submitBuild(self, jsversion = '2.0.2', arch = 'x86'):
        buildData = self.getJsonBuild(jsversion = jsversion)
        self.client.submitJob(buildData)
        self.mcp.commandQueue.incoming = self.client.command.outgoing
        self.client.command.outgoing = []

    def ackJobSlave(self, arch = 'x86'):
        demandName = 'demand:' + arch
        dataStr = ''
        if demandName in self.mcp.demand:
            if self.mcp.demand[demandName].outgoing:
                dataStr = self.mcp.demand[demandName].outgoing.pop()
            else:
                self.fail("demand queue %s is empty" % demandName)
        else:
            self.fail("demand queue %s doesn't exist" % demandName)
        data = simplejson.loads(dataStr)
        troveSpec = data['troveSpec']
        nvf = cmdline.parseTroveSpec(troveSpec)
        version = nvf[1].split('/')[1]
        data = {}
        slaveId = '%s:slave%d' % (self.masterResponse.node, self.slaveId)
        self.slaveId += 1
        for event in (slavestatus.BUILDING, slavestatus.STARTED):
            self.masterResponse.slaveStatus(slaveId, event,
                                            "%s:%s" % (version, arch))
            self.mcp.responseTopic.incoming.insert( \
                0, self.masterResponse.response.outgoing.pop())
        return slaveId

    def stopJobSlave(self, version, slaveId, arch = 'x86'):
        self.masterResponse.slaveStatus( \
            slaveId, slavestatus.OFFLINE, "%s:%s" % (version, arch))
        self.mcp.responseTopic.incoming.insert( \
            0, self.masterResponse.response.outgoing.pop())

    def testGetSuffix(self):
        assert server.getSuffix("1#x86") == 'x86'
        assert server.getSuffix("1#x86:~i486:~i586:~i686:~sse2|1#x86_64") == 'x86_64'
        assert server.getSuffix("") == ""

    def testMarshallX86Jobs(self):
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        assert self.mcp.jobQueues, "Job wasn't marshalled to any queue"
        queueName = [x for x in self.mcp.jobQueues if x.startswith('job')][0]
        assert queueName == 'job2.0.2-1-1:x86'
        self.mcp.jobQueues['job2.0.2-1-1:x86'].outgoing == self.getJsonBuild()

    def testDemandSlave(self):
        self.submitBuild()
        self.mcp.checkIncomingCommands()

        assert 'demand:x86' in self.mcp.demand

        dataStr = self.mcp.demand['demand:x86'].outgoing.pop()

        data = simplejson.loads(dataStr)

        nvf = cmdline.parseTroveSpec(data['troveSpec'])
        assert nvf[0] == self.cfg.slaveTroveName
        assert '2.0.2' in nvf[1]
        assert not dataStr.endswith('[None]')

    def testJobSlaveCountUp(self):
        assert self.mcp.jobSlaves == {}
        buildData = self.getJsonBuild()
        self.submitBuild()
        self.mcp.checkIncomingCommands()

        assert self.mcp.jobSlaves == {}

        self.ackJobSlave()

        self.mcp.checkResponses()

        assert self.mcp.jobSlaveCounts == {'2.0.2-1-1:x86' : 1}

    def testJobSlaveCountDown(self):
        self.mcp.jobSlaveCounts = {'2.0.2-1-1:x86' : 1}
        self.mcp.jobMasters = {'master': {'arch' : 'x86',
                                          'limit': 2,
                                          'slaves': ['master:slave0']}}
        self.mcp.jobSlaves = {'master:slave0' : {'type' : '2.0.2-1-1:x86',
                                                 'status' : 'idle',
                                                 'jobId': None}}

        self.stopJobSlave('2.0.2-1-1', 'master:slave0', arch = 'x86')

        self.mcp.checkResponses()

        assert self.mcp.jobSlaveCounts == {'2.0.2-1-1:x86' : 0}

    def testJobSlaveCount(self):
        assert self.mcp.jobSlaves == {}
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.ackJobSlave()
        self.mcp.checkResponses()
        self.mcp.jobSlaves == {'2.0.2-1-1:x86': 1}


    def testStartSlaveStatus(self):
        assert self.mcp.jobMasters == {}
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.failIf('demand:x86' not in self.mcp.demand,
                    "expected 'demand:x86' in mcp demands but found %s" % \
                        ', '.join(self.mcp.demand.keys()))
        slaveId = self.ackJobSlave()
        self.mcp.checkResponses()

        assert 'master' in self.mcp.jobMasters
        assert 'master:slave0' in self.mcp.jobMasters['master']['slaves']
        assert self.mcp.jobSlaves['master:slave0'] == \
            {'status': slavestatus.IDLE, 'type': '2.0.2-1-1:x86', 'jobId': None}

    def testCommandResponse(self):
        build = self.getJsonBuild()
        data = {}
        # test an illegal command
        data['action'] = 'startJob'
        data['data'] = build
        data['uuid'] = 'fake_uuid'
        data['protocolVersion'] = 1

        self.mcp.commandQueue.incoming = [simplejson.dumps(data)]
        self.mcp.checkIncomingCommands()

        assert self.mcp.postQueue.outgoing
        res = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert res == [True, ['IllegalCommand', 'Unknown action: startJob']]

        # now test a legal command
        data['action'] = 'submitJob'
        self.mcp.commandQueue.incoming = [simplejson.dumps(data)]
        self.mcp.checkIncomingCommands()

        assert self.mcp.postQueue.outgoing
        res = simplejson.loads(self.mcp.postQueue.outgoing.pop())

        assert res == [False, u'test.rpath.local:build-0']

    def testBrokenCommands(self):
        self.mcp.commandQueue.incoming = [simplejson.dumps('absolutelyWrong')]
        self.mcp.checkIncomingCommands()

        self.assertLogContent('command is not a dict')

        self.mcp.commandQueue.incoming = \
            [simplejson.dumps({'absolutely' : 'Wrong'})]
        self.mcp.checkIncomingCommands()
        self.assertLogContent('no post address')

        self.mcp.commandQueue.incoming = \
            [simplejson.dumps({'absolutely' : 'Wrong', 'uuid' : 'bad'})]
        self.mcp.checkIncomingCommands()

        assert simplejson.loads(self.mcp.postQueue.outgoing.pop()) == \
            [True, ['InternalServerError',
                    "An internal server error has occured"]]

        self.mcp.commandQueue.incoming = ['worst yet']
        self.mcp.checkIncomingCommands()

        self.assertLogContent('No JSON object could be decoded')

    def testDemandReturnCode(self):
        res = self.mcp.demandJobSlave('1.0.1-1-1', 'x86_64')
        assert res
        assert len(self.mcp.demand['demand:x86_64'].outgoing) == 1
        demanded = self.mcp.demand['demand:x86_64'].outgoing[0]
        assert '1.0.1-1-1' in demanded
        res = self.mcp.demandJobSlave('1.0.1-1-1', 'x86_64')
        assert not res
        assert len(self.mcp.demand['demand:x86_64'].outgoing) == 1

    def testJobConflict(self):
        build = self.getJsonBuild()
        self.client.submitJob(build)
        dataCommand = self.client.command.outgoing[0]
        self.mcp.commandQueue.incoming = [dataCommand]
        self.mcp.checkIncomingCommands()
        res = self.mcp.postQueue.outgoing.pop()

        assert res == '[false, "test.rpath.local:build-0"]'
        assert self.mcp.commandQueue.incoming == []

        self.mcp.commandQueue.incoming = [dataCommand]
        self.mcp.checkIncomingCommands()
        res = self.mcp.postQueue.outgoing.pop()
        assert res == '[true, ["JobConflict", "Job already in progress"]]'

    def testUnknownJob(self):
        build = simplejson.loads(self.getJsonBuild())
        build['type'] = 'random'
        self.client.submitJob(simplejson.dumps(build))
        dataCommand = self.client.command.outgoing[0]
        self.mcp.commandQueue.incoming = [dataCommand]
        self.mcp.checkIncomingCommands()
        res = self.mcp.postQueue.outgoing.pop()

        assert res == '[true, ["UnknownJobType", "Unknown job type: random"]]'

    def testBadJobData(self):
        build = simplejson.loads(self.getJsonBuild())
        build['type'] = 'random'
        self.client.submitJob(build)
        dataCommand = self.client.command.outgoing[0]
        self.mcp.commandQueue.incoming = [dataCommand]
        self.mcp.checkIncomingCommands()
        res = self.mcp.postQueue.outgoing.pop()

        self.failUnlessEqual(res, '[true, ["ProtocolError", "unable to parse job: expected string or buffer"]]')

    def testSetSlaveTTL(self):
        self.mcp.jobSlaves = {'master:slave' : {'status' : 'running',
                                                'jobId' : 'rogueJob',
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.setSlaveTTL('master:slave', 0)
        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
        self.checkValue(control, 'node', 'master:slave')
        self.checkValue(control, 'action', 'setTTL')
        self.checkValue(control, 'TTL', 0)
        assert 'protocolVersion' in control

    def testUnknownHostTTL(self):
        self.assertRaises(mcp_error.UnknownHost,
                          self.mcp.setSlaveTTL, 'unknown', 0)


    def testUnknownHostStopSlave(self):
        self.assertRaises(mcp_error.UnknownHost,
                          self.mcp.stopSlave, 'unknown', delayed = True)

    def testStopSlaveDelayed(self):
        self.mcp.jobSlaves = {'master:slave' : {'status' : 'running',
                                                'jobId' : 'rogueJob',
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.stopSlave('master:slave', delayed = True)

        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
        # a delayed stop slave is the exact same thing as a TTL of zero
        self.checkValue(control, 'node', 'master:slave')
        self.checkValue(control, 'action', 'setTTL')
        self.checkValue(control, 'TTL', 0)
        assert 'protocolVersion' in control

    def testStopSlaveImmediate(self):
        self.mcp.jobSlaves = {'master:slave' : {'status' : 'running',
                                                'jobId' : 'rogueJob',
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.stopSlave('master:slave', delayed = False)

        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
        self.checkValue(control, 'node', 'master')
        self.checkValue(control, 'action', 'stopSlave')
        self.checkValue(control, 'slaveId', 'master:slave')
        assert 'protocolVersion' in control

    def testSetSlaveLimitHost(self):
        self.assertRaises(mcp_error.UnknownHost, self.mcp.setSlaveLimit,
                          'master', 2)

    def testSetSlaveLimit(self):
        self.mcp.jobMasters = {'master' : {'slaves' : [],
                                           'arch' : 'x86',
                                           'limit' : 4}}

        self.mcp.setSlaveLimit('master', 2)
        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
        self.checkValue(control, 'node', 'master')
        self.checkValue(control, 'action', 'slaveLimit')
        self.checkValue(control, 'limit', 2)
        assert 'protocolVersion' in control


    def testStopJobUnk(self):
        self.assertRaises(mcp_error.UnknownJob,
                          self.mcp.stopJob, 'test.rpath.local:build-22')

    def testStopJob(self):
        build = self.getJsonBuild()
        self.mcp.jobSlaves = \
            {'master:slave' : {'status' : 'running',
                               'jobId' : 'test.rpath.local:build-22',
                               'type' : '1.0.4-12-3:x86'}}
        self.mcp.jobs = \
            {'test.rpath.local:build-22' : {'data' : build,
                                            'status' : 'running',
                                            'slaveId' : 'master:slave'}}
        self.mcp.stopJob('test.rpath.local:build-22')
        control = simplejson.loads(self.mcp.jobControlQueue.outgoing.pop())
        self.checkValue(control, 'node', 'slaves')
        self.checkValue(control, 'action', 'stopJob')
        self.checkValue(control, 'jobId', 'test.rpath.local:build-22')

        assert 'protocolVersion' in control

    def testHandleComMissing(self):
        self.mcp.handleCommand({})
        self.assertLogContent('no post address')

    def testHandleComProtocol(self):
        self.mcp.handleCommand({'protocolVersion' : 999999999999, 'uuid' : ''})
        res = self.mcp.postQueue.outgoing.pop()
        assert res == \
           '[true, ["ProtocolError", "Unknown Protocol Version: 999999999999"]]'

    def testJobLoad(self):
        self.mcp.jobCounts['1.2.3-4-5:x86'] = 2

        self.mcp.checkJobLoad()
        assert '1.2.3-4-5' in self.mcp.demand['demand:x86'].outgoing[0]
        assert self.mcp.demandCounts == {'1.2.3-4-5:x86': 1}

        self.mcp.checkJobLoad()
        assert len(self.mcp.demand['demand:x86'].outgoing) == 1
        assert self.mcp.demandCounts == {'1.2.3-4-5:x86': 1}


    def testJobLoad2(self):
        self.mcp.jobCounts['1.2.3-4-5:x86'] = 2
        self.mcp.jobSlaveCounts['1.2.3-4-5:x86'] = 2

        self.mcp.checkJobLoad()
        'demand:x86' not in self.mcp.demand
        assert self.mcp.demandCounts.get('1.2.3-4-5:x86', 0) == 0

        self.mcp.jobCounts['1.2.3-4-5:x86'] += 1
        self.mcp.checkJobLoad()
        assert len(self.mcp.demand['demand:x86'].outgoing) == 1
        assert self.mcp.demandCounts == {'1.2.3-4-5:x86': 1}

    def testRespawn(self):
        build = self.getJsonBuild()
        self.mcp.jobSlaves = \
            {'master:slave' : {'status' : 'running',
                               'jobId' : 'test.rpath.local:build-22',
                               'type' : '1.0.4-12-3:x86'}}
        self.mcp.jobs = \
            {'test.rpath.local:build-22' : {'data' : build,
                                            'status' : 'running',
                                            'slaveId' : 'master:slave'}}

        assert not self.mcp.jobQueues
        assert not self.mcp.jobCounts
        self.mcp.respawnJob('master:slave')
        self.assertLogContent('Respawn')
        assert self.mcp.jobQueues['job2.0.2-1-1:x86'].outgoing[0] == build
        assert self.mcp.jobCounts == {'2.0.2-1-1:x86': 1}

    def testRespawnData(self):
        build = self.getJsonBuild()
        self.mcp.jobSlaves = {'master:slave' : {'status' : 'running',
                                                'jobId' : None,
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.respawnJob('master:slave')
        assert not self.mcp.jobQueues
        assert not self.mcp.jobCounts

    def testSlaveOffline(self):
        self.mcp.jobSlaves = {'master:slave' : {'status' : 'running',
                                                'jobId' : None,
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.slaveOffline('master:slave')
        assert self.mcp.jobSlaves == {}

    def testGetSlave(self):
        self.mcp.getSlave('master:slave')
        assert self.mcp.jobMasters == \
            {'master': {'limit': None,
                        'arch': None,
                        'slaves': ['master:slave']}}
        assert self.mcp.jobSlaves == {'master:slave': {'status': None,
                                                       'type': None,
                                                       'jobId': None}}

    def testUnknownMaster(self):
        self.mcp.getMaster('master')
        assert self.mcp.jobMasters == \
            {'master': {'limit': None, 'arch': None, 'slaves': []}}

        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
        self.checkValue(control, 'action', 'status')
        self.checkValue(control, 'node', 'master')
        assert 'protocolVersion' in control


    def testUnknownSlave(self):
        self.mcp.getSlave('master:slave')
        # master would have been requested first
        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
        self.checkValue(control, 'action', 'status')
        self.checkValue(control, 'node', 'master')
        assert 'protocolVersion' in control

        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
        self.checkValue(control, 'action', 'status')
        self.checkValue(control, 'node', 'master:slave')
        assert 'protocolVersion' in control

    def testBadJobId(self):
        self.mcp.handleCommand({'protocolVersion' : 1, 'uuid' : 'test',
                                'action' : 'jobStatus',
                                'jobId' : 'bad-job-id'})
        res = self.mcp.postQueue.outgoing.pop()
        assert res == '[true, ["UnknownJob", "Unknown job Id: bad-job-id"]]'

    def testMissingJobId(self):
        self.mcp.handleCommand({'protocolVersion' : 1, 'uuid' : 'test',
                                'action' : 'jobStatus'})
        res = self.mcp.postQueue.outgoing.pop()
        assert res == '[false, {}]'

    def testClearCache(self):
        self.mcp.handleCommand({'protocolVersion' : 1, 'uuid' : 'test',
                                'action' : 'clearCache',
                                'masterId' : 'testMaster'})
        res = self.mcp.postQueue.outgoing.pop()

        assert res == '[true, ["UnknownHost", "Unknown Host: testMaster"]]'
        self.mcp.getMaster('testMaster')
        self.mcp.controlTopic.outgoing = []
        self.mcp.handleCommand({'protocolVersion' : 1, 'uuid' : 'test',
                                'action' : 'clearCache',
                                'masterId' : 'testMaster'})
        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
        self.checkValue(control, 'action', 'clearImageCache')
        self.checkValue(control, 'node', 'testMaster')

    def testLogJob(self):
        assert not self.mcp.logFiles
        self.mcp.logJob('dummy-build-26', 'test message')
        assert self.mcp.logFiles
        self.failUnless('dummy-build-26' in self.mcp.logFiles)
        self.mcp.logFiles['dummy-build-26'].close()

        logPath = self.mcp.cfg.logPath
        try:
            self.mcp.cfg.logPath = None
            out, err = self.captureOutput(self.mcp.logJob,
                                         'dummy-build-26', 'test message')
            self.failUnlessEqual(out, 'dummy-build-26: test message\n')
            self.failUnlessEqual(err, '')
        finally:
            self.mcp.cfg.logPath = logPath

    def testGetVersion(self):
        # mock out the conaryclient object to catch the repos call
        ConaryClient = conaryclient.ConaryClient
        class MockClient(ConaryClient):
            class MockRepos(object):
                def findTrove(self, *args, **kwargs):
                    print args, kwargs
                    return (('dummy', 'version', None),)
            def __init__(self, *args, **kw):
                ConaryClient.__init__(self, *args, **kw)
                self.repos = self.MockRepos()

        try:
            conaryclient.ConaryClient = MockClient
            out, err = \
                self.captureOutput(server.MCPServer.getVersion, self.mcp, '')
            refOut = "(None, ('group-core', 'products.rpath.com@rpath:js', " \
                "None)) {}\n"
            self.failIf(out != refOut, "findTrove expected: %s but got: %s" % \
                            (refOut, out))
        finally:
            conaryclient.ConaryClient = ConaryClient


    def testGetMissingVersion(self):
        class MockClient(object):
            class MockRepos(object):
                def findTrove(self, *args, **kwargs):
                    raise TroveNotFound('Dummy Call')
            repos = MockRepos()

        ConaryClient = conaryclient.ConaryClient
        try:
            conaryclient.ConaryClient = MockClient
            res = server.MCPServer.getVersion(self.mcp, '')
            self.failIf(res != [], "Expected getVersion to return [] "
                        "but got: %s" % str(res))
        finally:
            conaryclient.ConaryClient = ConaryClient

    def testDummyCook(self):
        self.mcp.handleJob(simplejson.dumps({'type': 'cook',
                                             'UUID': 'dummy-cook-47',
                                             'data' : {'arch': 'x86'}}))

        self.failIf('dummy-cook-47' not in self.mcp.jobs,
                    "Cook job was not recorded")

    def testLogErrors(self):
        class Foo(object):
            @server.logErrors
            def crash(self):
                raise AssertionError, 'Purposely raised'

        f = Foo()
        f.crash()

        self.assertLogContent('Purposely raised')

    def testDisconnect(self):
        class MockDisc(object):
            def __init__(self):
                self.connected = True
            def disconnect(self):
                self.connected = False

        self.mcp.commandQueue = MockDisc()
        self.mcp.responseTopic = MockDisc()
        self.mcp.controlTopic = MockDisc()
        self.mcp.demand['demand:x86'] = MockDisc()
        self.mcp.jobQueues['job3.0.0-1-1:x86'] = MockDisc()
        self.mcp.postQueue = MockDisc()

        self.mcp.running = True
        self.mcp.disconnect()
        self.failIf(self.mcp.commandQueue.connected, "Command Queue was not disconnected")
        self.failIf(self.mcp.responseTopic.connected,
                    "Response Topic was not disconnected")
        self.failIf(self.mcp.controlTopic.connected,
                    "Control Topic was not disconnected")
        self.failIf(self.mcp.demand['demand:x86'].connected,
                    "Demand Queue was not disconnected")
        self.failIf(self.mcp.jobQueues['job3.0.0-1-1:x86'].connected,
                    "Demand Queue was not disconnected")
        self.failIf(self.mcp.postQueue.connected,
                    "Post Queue was not disconnected")

    def testUnkJobStart(self):
        jobId = 'dummy-build-45'
        slaveId = 'master:slave'
        self.mcp.handleResponse({'node' : slaveId,
                                 'protocolVersion' : 1,
                                 'event' : 'jobStatus',
                                 'jobId' : jobId,
                                 'status' : jobstatus.RUNNING,
                                 'statusMessage' : ''})
        self.failIf('master:slave' not in self.mcp.jobSlaves,
                    "slave was not recorded through jobStatus")
        self.failIf('master' not in self.mcp.jobMasters,
                    "master was not recorded through jobStatus")
        self.failIf(jobId not in self.mcp.jobs, "job was not recorded")
        self.failIf(self.mcp.jobs[jobId]['slaveId'] != slaveId,
                    "job was not associated with it's slave")
        self.failIf(self.mcp.jobSlaves[slaveId]['jobId'] != jobId,
                    "slave was not associtated with it's job")

    def testKnownJobStart(self):
        jobId = 'dummy-build-21'
        slaveId = 'master:slave'

        self.mcp.jobs = {jobId : {'status' : (jobstatus.WAITING, ''),
                                  'data' : None,
                                  'slaveId': None}}
        self.mcp.jobSlaves = {'master:slave': {'status' : slavestatus.IDLE,
                                               'type': '3.0.0-1-1:x86',
                                               'jobId' : None}}

        self.mcp.jobCounts = {'3.0.0-1-1:x86': 2}

        self.mcp.handleResponse({'node' : slaveId,
                                 'protocolVersion' : 1,
                                 'event' : 'jobStatus',
                                 'jobId' : jobId,
                                 'status' : jobstatus.RUNNING,
                                 'statusMessage' : ''})
        self.failIf('master:slave' not in self.mcp.jobSlaves,
                    "slave was not recorded through jobStatus")
        self.failIf('master' not in self.mcp.jobMasters,
                    "master was not recorded through jobStatus")
        self.failIf(jobId not in self.mcp.jobs, "job was not recorded")
        self.failIf(self.mcp.jobs[jobId]['slaveId'] != slaveId,
                    "job was not associated with it's slave")
        self.failIf(self.mcp.jobSlaves[slaveId]['jobId'] != jobId,
                    "slave was not associtated with it's job")

        self.failIf(self.mcp.jobCounts != {'3.0.0-1-1:x86': 1},
                    "job count was not decremented properly")

    def testStopJobStatus(self):
        jobId = 'dummy-build-23'
        slaveId = 'master:slave'

        self.mcp.jobs = {jobId : {'status' : (jobstatus.RUNNING, ''),
                                  'data' : None,
                                  'slaveId': slaveId}}

        self.mcp.jobSlaves = {'master:slave': {'status' : slavestatus.ACTIVE,
                                               'type': '3.0.0-1-1:x86',
                                               'jobId' : jobId}}

        self.mcp.logFiles = {jobId: 'dummy logfile'}
        self.mcp.handleResponse({'node' : slaveId,
                                 'protocolVersion' : 1,
                                 'event' : 'jobStatus',
                                 'jobId' : jobId,
                                 'status' : jobstatus.FINISHED,
                                 'statusMessage' : ''})

        self.failIf(self.mcp.logFiles,
                    "Log file handler should have been removed")

        self.failIf(self.mcp.jobs[jobId]['slaveId'] != None,
                    "job was not disassociated with it's slave upon compeltion")

        self.failIf(self.mcp.jobSlaves[slaveId]['jobId'] != None,
                    "slave was not disassociated with it's job upon completion")

    def testJobLog(self):
        jobId = 'dummy-build-96'
        slaveId = 'master:slave'
        self.mcp.handleResponse({'node' : slaveId,
                                 'protocolVersion' : 1,
                                 'event' : 'jobLog',
                                 'jobId' : jobId,
                                 'message' : 'fake message'})
        self.failIf(jobId not in self.mcp.logFiles, "log file was not opened")

    def testPostJobOutput(self):
        jobId = 'dummy-build-32'
        slaveId = 'master:slave'
        self.mcp.handleResponse({'node' : slaveId,
                                 'protocolVersion' : 1,
                                 'event' : 'postJobOutput',
                                 'jobId' : jobId,
                                 'urls' : ['http://fake/1', 'test image'],
                                 'dest' : 'fake'})

        data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        self.failUnlessEqual(data, {'uuid': 'dummy-build-32',
                                    'urls': ['http://fake/1', 'test image']})

    def testBadResponseProtocol(self):
        jobId = 'dummy-build-54'
        slaveId = 'master:slave'
        self.mcp.handleResponse({'node' : slaveId,
                                 'protocolVersion' : 999999})

        self.assertLogContent('Unknown Protocol Version: 999999')

    def testMasterOffline(self):
        masterId = 'testmaster'
        slaveId = masterId + ':slave'
        self.mcp.jobMasters = {masterId: {'limit' : 1, 'arch': 'x86',
                                          'slaves' : [slaveId]}}
        self.mcp.jobSlaves = {slaveId : {'status' : 'idle',
                                         'type' : '3.0.0-1-1:x86',
                                         'jobId' : None}}
        self.mcp.handleResponse({'node' : masterId,
                                 'protocolVersion' : 1,
                                 'event' : 'masterOffline'})

        self.failIf(self.mcp.jobMasters, "Master was not removed")
        self.failIf(self.mcp.jobSlaves, "Slave was not removed with master")

    def testMasterStatus(self):
        masterId = 'testmaster'
        slaveId = masterId + ':slave'
        self.mcp.jobMasters = {masterId: {'limit' : 1, 'arch': 'x86',
                                          'slaves' : [slaveId]}}
        self.mcp.jobSlaves = {slaveId : {'status' : 'idle',
                                         'type' : '3.0.0-1-1:x86',
                                         'jobId' : None}}

        self.mcp.handleResponse({'node' : masterId,
                                 'protocolVersion' : 1,
                                 'event' : 'masterStatus',
                                 'arch' : 'x86',
                                 'limit' : 1,
                                 'slaves' : []})

        self.failIf(self.mcp.jobSlaves,
                    "Slave was not removed when master reported it missing")
        self.failIf(self.mcp.jobMasters[masterId]['slaves'],
                    "Slave was not disassociated from master")


    def testCommandJSVersion(self):
        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'getJSVersion'})
        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())

        assert not err
        # this data comes from the test suite. overloaded getVersion
        # see mcp_helper
        assert data == '3.0.0'


    def testCommandSlaveStatus(self):
        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'nodeStatus'})

        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert not err
        assert data == {}

    def testCommandJobStatus(self):
        jobId = 'dummy-cook-1'
        self.mcp.jobs = {jobId : {'status' : ('running', ''),
                                  'data' : None, 'slaveId' : None}}
        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'jobStatus',
                                'jobId' : jobId})

        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert not err
        assert data == ['running', '']

    def testCommandStopMaster(self):
        masterId = 'master64'
        slaveId = masterId + ':slave'
        self.mcp.jobMasters = {masterId : {'limit' : 1,
                                           'arch' : 'x86_64',
                                           'slaves' : [slaveId]}}
        self.mcp.jobSlaves = {slaveId: {'status' : 'idle',
                                        'type' : '3.0.0-1-1', 'jobId' : None}}

        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'stopMaster',
                                'masterId' : masterId,
                                'delayed' : False})

        assert self.mcp.controlTopic.outgoing == \
            ['{"node": "master64", "action": "stopSlave", '
             '"slaveId": "master64:slave", "protocolVersion": 1}',
             '{"node": "master64", "action": "slaveLimit", "limit": 0, '
             '"protocolVersion": 1}']

        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert not err
        assert data is None

    def testCommandStopUnkMaster(self):
        masterId = 'master64'

        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'stopMaster',
                                'masterId' : masterId,
                                'delayed' : False})
        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert err
        assert data == ['UnknownHost', 'Unknown Host: %s' % masterId]

    def testCommandStopUnkSlave(self):
        masterId = 'master64'
        slaveId = masterId + ":slave"

        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'stopSlave',
                                'slaveId' : slaveId,
                                'delayed' : False})
        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert err
        assert data == ['UnknownHost', 'Unknown Host: %s' % slaveId]

    def testCommandStopJob(self):
        jobId = 'dummy-build-54'

        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'stopJob',
                                'jobId' : jobId})

        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert err
        self.failIf(data != ['UnknownJob', 'Unknown Job ID: %s' % jobId],
                    "Job was not reported as unknown")

    def testCommandSetSlaveLimit(self):
        masterId = 'master64'

        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'setSlaveLimit',
                                'masterId' : masterId,
                                'limit' : 1})

        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert err
        self.failIf(data != ['UnknownHost', 'Unknown Host: %s' % masterId],
                    "Host was not reported as unknown")

    def testCommandSetSlaveTTL(self):
        masterId = 'master'
        slaveId = masterId + ':slave2'

        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'setSlaveTTL',
                                'slaveId' : slaveId,
                                'TTL' : 500})

        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert err
        self.failIf(data != ['UnknownHost', 'Unknown Host: %s' % slaveId],
                    "Host was not reported as unknown")

    def testRunMcp(self):
        sleep = time.sleep
        class IterationComplete(Exception):
            pass

        def newSleep(*args, **kwargs):
            raise IterationComplete

        try:
            time.sleep = newSleep
            self.assertRaises(IterationComplete, self.mcp.run)
        finally:
            time.sleep = sleep


if __name__ == "__main__":
    testsuite.main()
