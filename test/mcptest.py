#!/usr/bin/python
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()
import simplejson
import socket
import StringIO
import time

import os, sys
import threading
import tempfile

from mcp import queue
from mcp import constants
from conary import conaryclient
from conary.conaryclient import cmdline
from conary import versions
from conary.errors import TroveNotFound
from conary.repository import trovesource
from conary.deps import deps
from conary.lib import util

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
        jobName = 'job:%s' % arch
        dataStr = ''
        if jobName in self.mcp.jobQueues:
            if self.mcp.jobQueues[jobName].outgoing:
                dataStr = self.mcp.jobQueues[jobName].outgoing.pop()
            else:
                self.fail("job queue %s is empty" % jobName)
        else:
            self.fail("job queue %s doesn't exist" % jobName)
        data = simplejson.loads(dataStr)
        jobSlaveNVF = data['jobSlaveNVF']
        nvf = cmdline.parseTroveSpec(jobSlaveNVF)
        version = str(versions.VersionFromString(nvf[1]).trailingRevision())
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
        assert queueName == 'job:x86'

    def testStartSlaveStatus(self):
        assert self.mcp.jobMasters == {}
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.failIf('job:x86' not in self.mcp.jobQueues,
                "expected 'job:x86' in jobQueues, but found %s" % \
                        ', '.join(self.mcp.jobQueues.keys()))
        slaveId = self.ackJobSlave()
        self.mcp.checkResponses()

        assert 'master' in self.mcp.jobMasters
        assert 'master:slave0' in self.mcp.jobMasters['master']['slaves']
        self.assertEquals(self.mcp.jobSlaves['master:slave0'],
            {'status': slavestatus.IDLE, 'type': '2.0.2-1-1:x86',
                    'jobId': None})

    def testSlavehandleKilledJobs(self):
        jobId = 'rogueJob'
        slaveId = 'master:slave'
        self.mcp.jobSlaves = {slaveId : {'status' : slavestatus.STARTED,
                                                'jobId' : jobId,
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.jobs = {jobId : {'data': '',
                                  'status': (jobstatus.KILLED, ''),
                                  'slaveId': slaveId}}
        self.mcp.handleKilledJobs(slaveId)
        self.failIf(self.mcp.jobs[jobId]['status'] != \
                (301, "Job killed at user's request"),
                "Job didn't transition from killed to failed")

    def testStartSlaveBlacklist(self):
        assert self.mcp.jobMasters == {}
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        jobId = self.mcp.jobs.keys()[0]
        slaveId = 'master:slave05'
        self.mcp.jobs[jobId]['status'] = \
                (jobstatus.KILLED, '')
        self.masterResponse.slaveStatus(slaveId, slavestatus.BUILDING,
                "%s:%s" % ('2.0.2', 'x86'), jobId = jobId)
        self.mcp.responseTopic.incoming.insert( \
            0, self.masterResponse.response.outgoing.pop())
        self.mcp.checkResponses()

        found = False
        while self.mcp.controlTopic.outgoing:
            dataStr = self.mcp.controlTopic.outgoing.pop()
            data = simplejson.loads(dataStr)
            if data == {"node": "master", "action": "stopSlave",
                    "slaveId": "master:slave05", "protocolVersion": 1}:
                found = True
        self.failIf(not found, "stopSlave not emitted when slave checked in")

    def testStopSlaveBlacklist(self):
        jobId = 'rogueJob'
        slaveId = 'master:slave'
        self.mcp.jobSlaves = {slaveId : {'status' : slavestatus.STARTED,
                                                'jobId' : jobId,
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.jobs = {jobId : {'data': '',
                                  'status': (jobstatus.KILLED, ''),
                                  'slaveId': slaveId}}

        self.masterResponse.slaveStatus(slaveId, slavestatus.OFFLINE,
                "%s:%s" % ('2.0.2', 'x86'), jobId = jobId)
        self.mcp.responseTopic.incoming.insert( \
            0, self.masterResponse.response.outgoing.pop())
        self.mcp.checkResponses()

        self.failIf(self.mcp.jobs[jobId]['status'][0] != jobstatus.FAILED,
                "Job was not recorded as failed when slave stopped")

    def testStartJobBlacklist(self):
        assert self.mcp.jobMasters == {}
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        jobId = self.mcp.jobs.keys()[0]
        self.mcp.jobs[jobId]['status'] = \
                (jobstatus.KILLED, '')
        self.slaveResponse.jobStatus(jobId, jobstatus.RUNNING, "doomed job")
        self.mcp.responseTopic.incoming.insert( \
            0, self.slaveResponse.response.outgoing.pop())
        self.mcp.checkResponses()

        found = False
        while self.mcp.controlTopic.outgoing:
            dataStr = self.mcp.controlTopic.outgoing.pop()
            data = simplejson.loads(dataStr)
            if data == {"node": "master", "action": "stopSlave", "slaveId":
                    "master:slave", "protocolVersion": 1}:
                found = True
        self.failIf(not found, "stopSlave not emitted when job checked in")

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

        self.failUnlessEqual(res, '[false, null]')

    def testUnknownHostStopSlave(self):
        self.assertRaises(mcp_error.UnknownHost,
                          self.mcp.stopSlave, 'unknown')

    def testStopSlave(self):
        self.mcp.jobSlaves = {'master:slave' : {'status' : slavestatus.STARTED,
                                                'jobId' : 'rogueJob',
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.stopSlave('master:slave')

        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
        self.checkValue(control, 'node', 'master')
        self.checkValue(control, 'action', 'stopSlave')
        self.checkValue(control, 'slaveId', 'master:slave')
        assert 'protocolVersion' in control

    def testStopJob(self):
        jobId = 'rogueJob'
        slaveId = 'master:slave'
        self.mcp.jobSlaves = {slaveId : {'status' : slavestatus.STARTED,
                                                'jobId' : jobId,
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.jobs = {jobId : {'data': '',
                                  'status': (100, ''),
                                  'slaveId': slaveId}}
        self.mcp.stopJob(jobId, useQueue = False)
        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())

        self.checkValue(control, 'node', 'slaves')
        self.checkValue(control, 'action', 'stopJob')
        self.checkValue(control, 'jobId', jobId)

    def testStopJobBlacklist(self):
        jobId = 'rogueJob'
        self.mcp.jobs = {jobId : {'data': '',
                                  'status': (jobstatus.WAITING, ''),
                                  'slaveId': None}}
        self.mcp.stopJob(jobId)
        self.failIf(self.mcp.jobs[jobId]['status'] != (jobstatus.KILLED,
            "Job killed at user's request"), "Expected job killed message")

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
            {'master:slave' : {'status' : slavestatus.STARTED,
                               'jobId' : 'test.rpath.local:build-22',
                               'type' : '1.0.4-12-3:x86'}}
        self.mcp.jobs = \
            {'test.rpath.local:build-22' : {'data' : build,
                                            'status' : jobstatus.RUNNING,
                                            'slaveId' : 'master:slave'}}
        self.mcp.stopJob('test.rpath.local:build-22')
        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
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

    def testRespawn(self):
        build = self.getJsonBuild()
        self.mcp.jobSlaves = \
            {'master:slave' : {'status' : slavestatus.STARTED,
                               'jobId' : 'test.rpath.local:build-22',
                               'type' : '1.0.4-12-3:x86'}}
        self.mcp.jobs = \
            {'test.rpath.local:build-22' : {'data' : build,
                                            'status' : (jobstatus.RUNNING, ''),
                                            'slaveId' : 'master:slave'}}

        assert not self.mcp.jobQueues
        self.mcp.respawnJob('master:slave')
        self.assertLogContent('Respawn')

        outBuild = simplejson.loads(self.mcp.jobQueues['job:x86'].outgoing[0])
        build = simplejson.loads(build)
        for key, val in build.iteritems():
            self.failIf(outBuild.get(key) != val,
                    "build data %s did not match expected value: %s" % \
                        (key, val))

    def testRespawnData(self):
        build = self.getJsonBuild()
        self.mcp.jobSlaves = {'master:slave' : {'status' : slavestatus.STARTED,
                                                'jobId' : None,
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.respawnJob('master:slave')
        assert not self.mcp.jobQueues

    def testSlaveOffline(self):
        jobId = 'rogueJob'
        self.mcp.jobSlaves = {'master:slave' : {'status' : slavestatus.STARTED,
                                                'jobId' : jobId,
                                                'type' : '1.0.4-12-3:x86'}}
        f = open(self.mcpBasePath + '/logfile', 'w')
        self.mcp.logFiles = {jobId: f}
        f.write("Hello World")

        self.mcp.slaveOffline('master:slave')

        self.failIf(jobId in self.mcp.logFiles,
            "Log file handler should have been removed")
        assert self.mcp.jobSlaves == {}

        # make sure job logfile is compressed
        self.failUnless(os.path.exists(self.mcpBasePath + '/logfile.gz'))

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
            ret, output = self.captureOutput(self.mcp.logJob,
                                             'dummy-build-26', 'test message')
            self.failUnlessEqual(output, 'dummy-build-26: test message\n')
        finally:
            self.mcp.cfg.logPath = logPath

    def testGetVersion(self):
        # mock out the conaryclient object to catch the repos call
        self.mcp.jobSlaveSource.addTrove(*mcp_helper.SlaveBits.trove)
        self.mcp.slaveInstallPath.add(mcp_helper.SlaveBits.label)
        try:
            res = server.MCPServer.getVersion(self.mcp, '')
            ref = mcp_helper.SlaveBits.trove
            self.failUnlessEqual(ref, res)
        finally:
            self.mcp.jobSlaveSource = trovesource.SimpleTroveSource()


    def testGetMissingVersion(self):
        ConaryClient = conaryclient.ConaryClient
        class MockClient(ConaryClient):
            class MockRepos(object):
                def findTrove(self, *args, **kwargs):
                    raise TroveNotFound('Dummy Call')
            def __init__(self, *args, **kw):
                ConaryClient.__init__(self, *args, **kw)
                self.repos = self.MockRepos()

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

    def testBadJsVersion(self):
        UUID =  'dummy-cook-47'
        # test a bogus jsversion to account for proper mcp reaction
        # actual version doesn't matter since the slaveSource is empty
        mcp = server.MCPServer(self.cfg)
        mcp.slaveInstallPath = set([mcp_helper.SlaveBits.label])
        mcp.handleJob(simplejson.dumps({'type': 'build',
                                             'UUID': UUID,
                                             'troveFlavor': 'x86',
                                             'data': {'jsversion': '0'}}))

        self.failIf(UUID not in mcp.jobs,
                    "Cook job was not recorded")

        self.failIf(mcp.jobs[UUID]['status'][0] != jobstatus.FAILED,
                "Expected job to fail on missing jobslave version")

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
        self.mcp.jobQueues['job:x86'] = MockDisc()
        self.mcp.postQueue = MockDisc()

        self.mcp.running = True
        self.mcp.disconnect()
        self.failIf(self.mcp.commandQueue.connected, "Command Queue was not disconnected")
        self.failIf(self.mcp.responseTopic.connected,
                    "Response Topic was not disconnected")
        self.failIf(self.mcp.controlTopic.connected,
                    "Control Topic was not disconnected")
        self.failIf(self.mcp.jobQueues['job:x86'].connected,
                    "Job Queue was not disconnected")
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

        self.failUnless(self.mcp.logFiles,
                    "Log file handler should not yet have been removed")

        self.failIf(self.mcp.jobs[jobId]['slaveId'] != None,
                    "job was not disassociated with its slave upon completion")

        self.failIf(self.mcp.jobSlaves[slaveId]['jobId'] != None,
                    "slave was not disassociated with its job upon completion")

    def testJobLog(self):
        jobId = 'dummy-build-96'
        slaveId = 'master:slave'
        self.mcp.handleResponse({'node' : slaveId,
                                 'protocolVersion' : 1,
                                 'event' : 'jobLog',
                                 'jobId' : jobId,
                                 'message' : 'fake message'})
        self.failIf(jobId not in self.mcp.logFiles, "log file was not opened")

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
        self.mcp.jobSlaves = {slaveId : {'status' : slavestatus.IDLE,
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
        self.mcp.jobSlaves = {slaveId : {'status' : slavestatus.IDLE,
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

    def testMasterStatus2(self):
        masterId = 'testmaster'
        slaveId = masterId + ':slave'
        self.mcp.jobMasters = {masterId: {'limit' : 1, 'arch': 'x86',
                                          'slaves' : []}}

        self.mcp.handleResponse({'node' : masterId,
                                 'protocolVersion' : 1,
                                 'event' : 'masterStatus',
                                 'arch' : 'x86',
                                 'limit' : 1,
                                 'slaves' : [slaveId]})

        self.failIf(not self.mcp.jobSlaves,
                    "Slave was not added when master reported it")
        self.failIf(not self.mcp.jobMasters[masterId]['slaves'],
                    "Slave was not assciated with master")

    def testMasterStatus3(self):
        masterId = 'testmaster'
        slaveId = masterId + ':slave'
        self.mcp.jobMasters = {masterId: {'limit' : 1, 'arch': 'x86',
                                          'slaves' : [slaveId]}}
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

    def testMasterStatus4(self):
        # have an invalid slave entry, not linked to the jobMaster. send a
        # status message indicating it doesn't exist
        masterId = 'testmaster'
        slaveId = masterId + ':slave'
        self.mcp.jobMasters = {masterId: {'limit' : 1, 'arch': 'x86',
                                          'slaves' : []}}
        self.mcp.jobSlaves = {slaveId : {'status' : slavestatus.IDLE,
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

    def testNodeStatus(self):
        masterId = 'master99'
        slaveId = 'master99:slave00'
        self.mcp.jobMasters = {masterId : {'limit' : 1,
                                           'arch' : 'x86_64',
                                           'slaves' : [slaveId]}}
        self.mcp.jobSlaves = {slaveId: {'status' : slavestatus.IDLE,
                                        'type' : '3.0.0-1-1', 'jobId' : None}}
        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'nodeStatus'})

        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert not err
        assert data == {'master99': {'arch': 'x86_64', 'limit': 1,
            'slaves': {'master99:slave00':
                {'status': 200, 'type': '3.0.0-1-1', 'jobId': None}}}}

    def testCommandJobStatus(self):
        jobId = 'dummy-cook-1'
        self.mcp.jobs = {jobId : {'status' : (jobstatus.RUNNING, ''),
                                  'data' : None, 'slaveId' : None}}
        self.mcp.handleCommand({'uuid' : '12345',
                                'protocolVersion' : 1,
                                'action' : 'jobStatus',
                                'jobId' : jobId})

        err, data = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        assert not err
        assert data == [jobstatus.RUNNING, '']

    def testCommandStopMaster(self):
        masterId = 'master64'
        slaveId = masterId + ':slave'
        self.mcp.jobMasters = {masterId : {'limit' : 1,
                                           'arch' : 'x86_64',
                                           'slaves' : [slaveId]}}
        self.mcp.jobSlaves = {slaveId: {'status' : slavestatus.IDLE,
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
                                'slaveId' : slaveId})
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

    def testRunMcp(self):
        sleep = time.sleep
        class IterationComplete(Exception):
            pass

        def newSleep(*args, **kwargs):
            raise IterationComplete

        stockSlaveSource = self.mcp.stockSlaveSource
        try:
            self.mcp.stockSlaveSource = lambda: None
            time.sleep = newSleep
            self.assertRaises(IterationComplete, self.mcp.run)
        finally:
            self.mcp.stockSlaveSource = stockSlaveSource
            time.sleep = sleep

    def testWaitingJobNumber(self):
        jobId = 'test.rpath.local:build-0'
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.failIf(self.mcp.waitingJobs != [jobId],
                "Job was not put into waitingJobs on submission")
        self.client.jobStatus(jobId)
        self.mcp.commandQueue.incoming = self.client.command.outgoing
        self.client.command.outgoing = []
        self.mcp.postQueue.outgoing = []
        self.mcp.checkIncomingCommands()
        error, res = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        self.failIf(error, "unexpected error checking jobStatus: %s" % res)
        status, statusMessage = res
        self.failIf(status != jobstatus.WAITING,
                "Expected status %d, but got %d" % (jobstatus.WAITING, status))
        self.failIf(statusMessage != "Next in line for processing",
                'Status message does not reflect place in line')

    def testWaitingJobNumber2(self):
        jobId = 'test.rpath.local:build-1'
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.client.jobStatus(jobId)
        self.mcp.commandQueue.incoming = self.client.command.outgoing
        self.client.command.outgoing = []
        self.mcp.postQueue.outgoing = []
        self.mcp.checkIncomingCommands()
        error, res = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        self.failIf(error, "unexpected error checking jobStatus: %s" % res)
        status, statusMessage = res
        self.failIf(status != jobstatus.WAITING,
                "Expected status %d, but got %d" % (jobstatus.WAITING, status))
        self.failIf(statusMessage != "Number 2 in line for processing",
                'Status message does not reflect place in line')

    def testWaitingJobDecrement(self):
        jobId0 = 'test.rpath.local:build-0'
        jobId1 = 'test.rpath.local:build-1'
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.slaveResponse.jobStatus(jobId0, jobstatus.RUNNING, "starting")
        self.mcp.responseTopic.incoming.insert( \
                    0, self.slaveResponse.response.outgoing.pop())
        self.mcp.checkResponses()
        self.client.jobStatus(jobId1)
        self.mcp.commandQueue.incoming = self.client.command.outgoing
        self.client.command.outgoing = []
        self.mcp.postQueue.outgoing = []
        self.mcp.checkIncomingCommands()
        error, res = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        self.failIf(error, "unexpected error checking jobStatus: %s" % res)
        status, statusMessage = res
        self.failIf(status != jobstatus.WAITING,
                "Expected status %d, but got %d" % (jobstatus.WAITING, status))
        self.failIf(statusMessage != "Next in line for processing",
                'Status message does not reflect place in line')

    def testWaitingJobMasking(self):
        jobId0 = 'test.rpath.local:build-0'
        jobId1 = 'test.rpath.local:build-1'
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.slaveResponse.jobStatus(jobId0, jobstatus.RUNNING, "starting")
        self.mcp.responseTopic.incoming.insert( \
                    0, self.slaveResponse.response.outgoing.pop())
        self.mcp.checkResponses()
        self.client.jobStatus(jobId0)
        self.mcp.commandQueue.incoming = self.client.command.outgoing
        self.client.command.outgoing = []
        self.mcp.postQueue.outgoing = []
        self.mcp.checkIncomingCommands()
        error, res = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        self.failIf(error, "unexpected error checking jobStatus: %s" % res)
        status, statusMessage = res
        self.failIf(status != jobstatus.RUNNING,
                "Expected status %d, but got %d" % (jobstatus.RUNNING, status))
        self.failIf(statusMessage != "starting",
                'Status message was masked by waiting logic')

    def testWaitingJobKilled(self):
        jobId = 'test.rpath.local:build-0'
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.failIf(self.mcp.waitingJobs != [jobId],
                "Job was not put into waitingJobs on submission")
        self.mcp.jobs[jobId]['status'] = (jobstatus.KILLED, "dead test job")
        self.client.jobStatus(jobId)
        self.mcp.commandQueue.incoming = self.client.command.outgoing
        self.client.command.outgoing = []
        self.mcp.postQueue.outgoing = []
        self.mcp.checkIncomingCommands()
        error, res = simplejson.loads(self.mcp.postQueue.outgoing.pop())
        self.failIf(error, "unexpected error checking jobStatus: %s" % res)
        status, statusMessage = res
        self.failIf(status != jobstatus.KILLED,
                "Expected status %d, but got %d" % (jobstatus.KILLED, status))
        self.failIf(statusMessage != "dead test job",
                "expected dead job, not a place in line")

    def testJobKillSlaveAssoc(self):
        jobId = 'test.rpath.local:build-0'
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.failIf(self.mcp.waitingJobs != [jobId],
                "Job was not put into waitingJobs on submission")
        self.mcp.jobs[jobId]['status'] = (jobstatus.KILLED, "dead test job")
        self.slaveResponse.jobStatus(jobId, jobstatus.RUNNING, 'started')
        self.mcp.responseTopic.incoming.append( \
                self.slaveResponse.response.outgoing.pop())
        self.mcp.checkResponses()
        slaveId = self.mcp.jobs[jobId]['slaveId']
        self.failIf(slaveId is None,
                "slave was not associated with job in kill scenario")
        newJobId = self.mcp.jobSlaves[slaveId]['jobId']
        self.failIf(jobId != newJobId, "job was not assigned to slave in kill scenario")

    def testJobKillWaitingEffect(self):
        jobId = 'test.rpath.local:build-0'
        slaveId = 'master:slave'
        self.submitBuild()
        self.mcp.checkIncomingCommands()
        self.failIf(self.mcp.waitingJobs != [jobId],
                "Job was not put into waitingJobs on submission")
        self.mcp.jobs[jobId]['status'] = (jobstatus.KILLED, "dead test job")
        self.mcp.getSlave(slaveId)
        self.mcp.jobs[jobId]['slaveId'] = slaveId
        self.mcp.jobSlaves[slaveId]['jobId'] = jobId
        self.masterResponse.slaveStatus(slaveId, slavestatus.OFFLINE, 'dummy')
        self.mcp.responseTopic.incoming.append(self.masterResponse.response.outgoing.pop())
        self.mcp.checkResponses()
        self.failIf(self.mcp.waitingJobs == [jobId],
                "Job was not removed from waiting queue on slave stop")

    def testStockSlaveSource(self):
        '''Population of jobslave stock'''
        ## mock out the conaryclient object to catch the repos call
        # NVF for jobslave-set we eventually return
        slaveset = ('group-jobslave-set', versions.VersionFromString( \
                '/products.rpath.com@rpath:js/12345-1-1'), \
                deps.parseFlavor(''))
        slavelabel = slaveset[1].trailingLabel().asString()
        # NVF for jobslave we eventually return
        jobslave = mcp_helper.SlaveBits.trove

        class MockSource(object):
            # Mocks the source used to look up the jobslave set
            def findTroves(self, query, **kwargs):
                assert len(query) == 1
                assert query[0][1] == slavelabel + '/12345'
                return {query[0]: [slaveset]}
        class MockClient(object):
            def iterTroveList(x, *args, **kwargs):
                yield jobslave
            def __init__(x, *args, **kw):
                x.db = x
                x.findTrove = lambda *args, **kwargs: [(1, 2, 3)]
                x.getRepos = lambda: x
                x.getTrove = lambda *args, **kwargs: x
                x.getSearchSource = lambda *args, **kwargs: MockSource()

        ConaryClient = conaryclient.ConaryClient
        try:
            conaryclient.ConaryClient = MockClient

            self.mcp.cfg.slaveTroveLabel = slavelabel
            self.mcp.stockSlaveSource()
            res = self.mcp.jobSlaveSource.findTrove( \
                    None, jobslave)
            self.failIf([jobslave] != res, "expected slaveSource to be stocked")
        finally:
            conaryclient.ConaryClient = ConaryClient
            self.mcp.slaveSource = trovesource.SimpleTroveSource()

    def testJobLogFailure(self):
        jobId = 'rogueJob'
        slaveId = 'master:slave'
        self.mcp.jobSlaves = {slaveId : {'status' : slavestatus.STARTED,
                                                'jobId' : jobId,
                                                'type' : '1.0.4-12-3:x86'}}
        self.mcp.jobs = {jobId : {'data': '',
                                  'status': (jobstatus.KILLED, ''),
                                  'slaveId': slaveId}}

        def BadExec(*args, **kwargs):
            raise OSError

        class BadFile(object):
            def __init__(self, name):
                self.name = name
            def close(self):
                pass

        tmpDir = tempfile.mkdtemp()
        logPath = os.path.join(tmpDir, 'not', 'there')
        self.mcp.logFiles[jobId] = BadFile(logPath)
        try:
            self.captureOutput(self.mcp.slaveOffline, slaveId)
            self.failIf(self.mcp.jobSlaves != {},
                    "Slave destrcution interrupted by logFile issues")
        finally:
            util.rmtree(tmpDir, ignore_errors = True)

    def testJobLogFailure2(self):
        jobId = 'rogueJob'
        self.mcp.jobs = {jobId : {'data': '',
                                  'status': (jobstatus.KILLED, ''),
                                  'slaveId': None}}

        tmpDir = tempfile.mkdtemp()
        logPath = os.path.join(tmpDir, 'not', 'there')
        savedLogPath = self.mcp.cfg.logPath
        try:
            self.mcp.cfg.logPath = logPath
            self.mcp.logJob(jobId, "test")
        finally:
            self.mcp.cfg.logPath = savedLogPath
            util.rmtree(tmpDir, ignore_errors = True)

    def testLocalDebug(self):
        DEBUG_PATH = server.DEBUG_PATH
        # make sure we don't trigger anything in epdb at all.
        class FakeEpdbModule(object):
            def __init__(x):
                x.st_called = False
            def st(x, cond = ''):
                x.st_called = True
        class FakeFile(object):
            closed = False
            isatty = lambda x: True
        epdbModule = server.epdb
        tmpDir = tempfile.mkdtemp()
        stdout = sys.stdout
        stderr = sys.stderr
        stdin = sys.stdin
        try:
            # we actually have to stub out the tty's so that this test can be
            # run by testsuite daemons
            sys.stdout = FakeFile()
            sys.stderr = FakeFile()
            sys.stdin = FakeFile()
            server.epdb = FakeEpdbModule()
            server.DEBUG_PATH = tmpDir
            self.mcp.checkDebug()
            self.failIf(not server.epdb.st_called,
                    "Expected epdb.st to have been called")
        finally:
            sys.stdout = stdout
            sys.stderr = stderr
            sys.stdin = stdin
            util.rmtree(tmpDir)
            server.DEBUG_PATH = DEBUG_PATH
            server.epdb = epdbModule

    def testRemoteDebug(self):
        DEBUG_PATH = server.DEBUG_PATH
        # make sure we don't trigger anything in epdb at all.
        class FakeEpdbModule(object):
            def __init__(x):
                x.serve_called = False
            def serve(x, cond = ''):
                x.serve_called = True
        epdbModule = server.epdb
        tmpDir = tempfile.mkdtemp()
        stdOut = os.dup(sys.stdout.fileno())
        try:
            devNull = os.open(os.devnull, os.W_OK)
            os.dup2(devNull, sys.stdout.fileno())
            os.close(devNull)
            server.epdb = FakeEpdbModule()
            server.DEBUG_PATH = tmpDir
            self.mcp.checkDebug()
            self.failIf(not server.epdb.serve_called,
                    "Expected epdb.serve to have been called")
        finally:
            os.dup2(stdOut, sys.stdout.fileno())
            os.close(stdOut)
            util.rmtree(tmpDir)
            server.DEBUG_PATH = DEBUG_PATH
            server.epdb = epdbModule

    def testDebugFallback(self):
        DEBUG_PATH = server.DEBUG_PATH
        # make sure we don't trigger anything in epdb at all.
        class FakeEpdbModule(object):
            def __init__(x):
                x.st_called = False
            def st(x, cond = ''):
                x.st_called = True
            def serve(x):
                raise socket.error
        epdbModule = server.epdb
        tmpDir = tempfile.mkdtemp()
        stdOut = os.dup(sys.stdout.fileno())
        try:
            devNull = os.open(os.devnull, os.W_OK)
            os.dup2(devNull, sys.stdout.fileno())
            os.close(devNull)
            server.epdb = FakeEpdbModule()
            server.DEBUG_PATH = tmpDir
            self.mcp.checkDebug()
            self.failIf(not server.epdb.st_called,
                    "Expected epdb.serve to have been called")
        finally:
            os.dup2(stdOut, sys.stdout.fileno())
            os.close(stdOut)
            util.rmtree(tmpDir)
            server.DEBUG_PATH = DEBUG_PATH
            server.epdb = epdbModule

    def testSlavesetLabel(self):
        '''Retrieve jobslave-set label'''
        class DummyFile:
            def __init__(foo, path, *P, **K):
                self.assertEquals(path, '/etc/sysconfig/appliance-group')
            def read(foon):
                return 'group-hug\n'
        class DeadFile:
            def __init__(foo, path, *P, **K):
                raise IOError, 'CAN HAS FILE?'
        class DummyClient:
            def __init__(xself, trove, rev):
                xself.db = xself # make cc.db.findTrove work
                xself.trove, xself.rev = trove, rev
            def findTrove(xself, path, spec, *P, **K):
                self.assertEquals(spec[0], xself.trove)
                return [(xself.trove, versions.VersionFromString(xself.rev),
                    deps.parseFlavor(''))]

        # set up
        oldLabel, self.cfg.slaveSetLabel = self.cfg.slaveSetLabel, None
        import mcp.server

        # with appliance-group
        mcp.server.open = DummyFile
        label = self.mcp.getTopGroupLabel(DummyClient('group-hug',
            '/conary.rpath.com@rpl:devel//1/1.2.3-0.4-5'))
        self.assertEquals(label, 'conary.rpath.com@rpl:1')

        # without appliance-group
        mcp.server.open = DeadFile
        label = self.mcp.getTopGroupLabel(DummyClient('mcp',
            '/ted.danson@bean:cup/9-8-7'))
        self.assertEquals(label, 'ted.danson@bean:cup')

        # tear down
        self.cfg.slaveSetLabel = oldLabel
        mcp.server.open = open

if __name__ == "__main__":
    testsuite.main()
