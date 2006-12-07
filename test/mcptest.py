#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()
import simplejson

import os, sys
import threading
from mcp import queue
from conary.conaryclient import cmdline
from conary import versions

from mcp import server, mcp_error
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
        version = str(versions.VersionFromString(nvf[1]).trailingRevision())
        data = {}
        slaveId = '%s:slave%d' % (self.masterResponse.node, self.slaveId)
        self.slaveId += 1
        for event in ('building', 'running'):
            self.masterResponse.slaveStatus(slaveId, event,
                                            "%s:%s" % (version, arch))
            self.mcp.responseTopic.incoming.insert( \
                0, self.masterResponse.response.outgoing.pop())
        return slaveId

    def stopJobSlave(self, version, slaveId, arch = 'x86'):
        self.masterResponse.slaveStatus( \
            slaveId, 'offline', "%s:%s" % (version, arch))
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
            {'status': 'idle', 'type': '2.0.2-1-1:x86', 'jobId': None}

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

        assert self.mcp.responseTopic.outgoing
        res = simplejson.loads(self.mcp.responseTopic.outgoing.pop())
        assert res == [True, ['IllegalCommand', 'Unknown action: startJob']]

        # now test a legal command
        data['action'] = 'submitJob'
        self.mcp.commandQueue.incoming = [simplejson.dumps(data)]
        self.mcp.checkIncomingCommands()

        assert self.mcp.responseTopic.outgoing
        res = simplejson.loads(self.mcp.responseTopic.outgoing.pop())

        assert res == [False, u'test.rpath.local:build-0']

    def testBrokenCommands(self):
        self.mcp.commandQueue.incoming = [simplejson.dumps('absolutelyWrong')]
        self.mcp.checkIncomingCommands()

        self.assertLogContent('command is not a dict')

        self.mcp.commandQueue.incoming = \
            [simplejson.dumps({'absolutely' : 'Wrong'})]
        self.mcp.checkIncomingCommands()
        self.assertLogContent('no response address')

        self.mcp.commandQueue.incoming = \
            [simplejson.dumps({'absolutely' : 'Wrong', 'uuid' : 'bad'})]
        self.mcp.checkIncomingCommands()

        assert simplejson.loads(self.mcp.responseTopic.outgoing.pop()) == \
            [True, ['InternalServerError',
                    "An internal server error has occured"]]

        self.mcp.commandQueue.incoming = ['worst yet']
        self.mcp.checkIncomingCommands()

        self.assertLogContent('No JSON object could be decoded')

    def testLogFallback(self):
        from mcp import config
        cfg = config.MCPConfig()
        cfg.logPath = None
        mcp = server.MCPServer(cfg)
        mcp.log == sys.stderr


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
        res = self.mcp.responseTopic.outgoing.pop()

        assert res == '[false, "test.rpath.local:build-0"]'
        assert self.mcp.commandQueue.incoming == []

        self.mcp.commandQueue.incoming = [dataCommand]
        self.mcp.checkIncomingCommands()
        res = self.mcp.responseTopic.outgoing.pop()
        assert res == '[true, ["JobConflict", "Job already in progress"]]'

    def testUnknownJob(self):
        build = simplejson.loads(self.getJsonBuild())
        build['type'] = 'random'
        self.client.submitJob(simplejson.dumps(build))
        dataCommand = self.client.command.outgoing[0]
        self.mcp.commandQueue.incoming = [dataCommand]
        self.mcp.checkIncomingCommands()
        res = self.mcp.responseTopic.outgoing.pop()

        assert res == '[true, ["UnknownJobType", "Unknown job type: random"]]'

    def testBadJobData(self):
        build = simplejson.loads(self.getJsonBuild())
        build['type'] = 'random'
        self.client.submitJob(build)
        dataCommand = self.client.command.outgoing[0]
        self.mcp.commandQueue.incoming = [dataCommand]
        self.mcp.checkIncomingCommands()
        res = self.mcp.responseTopic.outgoing.pop()

        assert res == '[true, ["ProtocolError", "unable to parse job"]]'

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
        control = simplejson.loads(self.mcp.controlTopic.outgoing.pop())
        self.checkValue(control, 'node', 'master:slave')
        self.checkValue(control, 'action', 'stopJob')
        self.checkValue(control, 'jobId', 'test.rpath.local:build-22')

        assert 'protocolVersion' in control

    def testHandleComMissing(self):
        self.mcp.handleCommand({})
        self.assertLogContent('no response address')

    def testHandleComProtocol(self):
        self.mcp.handleCommand({'protocolVersion' : 999999999999, 'uuid' : ''})
        res = self.mcp.responseTopic.outgoing.pop()
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


    # test log handling
    # ensure stopMaster is atomic

    # test response commands out of order
    # test commands more thoroughly



if __name__ == "__main__":
    testsuite.main()
