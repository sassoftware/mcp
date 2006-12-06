#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()
import simplejson

from mcp import client
from mcp import mcp_error

import mcp_helper

class ClientTest(mcp_helper.MCPTest):
    def queueResponse(self, res, error = False):
        self.client.response.inbound.insert(0, simplejson.dumps((error, res)))

    def getCommand(self):
        if self.client.command.connection.sent:
            res = self.client.command.connection.sent.pop()[1]
            self.failIf('action' not in res, "command is missing action")
            return simplejson.loads(res)
        self.fail("No command was in queue")

    def testCheckValue(self):
        command = {'action' : 'testCommand'}
        self.assertRaises(AssertionError, self.checkValue, command,
                          'missingKey', '')
        self.assertRaises(AssertionError, self.checkValue, command,
                          'action', 'wrong')
        assert self.checkValue(command, 'action', 'testCommand') is None

    def testBasicAttributes(self):
        assert self.client.command.connectionName == '/queue/test/command'
        assert self.client.response.connectionName == \
            '/topic/test/' + self.client.uuid

        res = self.client.slaveStatus()
        assert self.client.command.connection.sent[0][0] == \
            '/queue/test/command'

    def testSubmitBuild(self):
        build = self.getJsonBuild()

        self.client.submitJob(build)
        assert self.client.command.connection.sent[0][0] == \
            '/queue/test/command'

        command = simplejson.loads(self.client.command.connection.sent[0][1])

        assert command['data'] == build
        assert self.client.response.connectionName == \
            '/topic/test/' + command['uuid']

        self.checkValue(command, 'action', 'submitJob')
        self.checkValue(command, 'protocolVersion', 1)

    def testSlaveStatus(self):
        self.queueResponse({})
        res = self.client.slaveStatus()
        assert res == {}

        command = self.getCommand()
        self.checkValue(command, 'action', 'slaveStatus')

    def testStopJob(self):
        self.queueResponse(None)
        self.client.stopJob('dummy-jobId')

        command = self.getCommand()
        self.checkValue(command, 'action', 'stopJob')
        self.checkValue(command, 'jobId', 'dummy-jobId')

    def testJobStatus(self):
        self.queueResponse(('failed', 'for no reason'))
        res = self.client.jobStatus('test-job')
        assert res == ['failed', 'for no reason']

        command = self.getCommand()
        self.checkValue(command, 'action', 'jobStatus')
        self.checkValue(command, 'jobId', 'test-job')

    def testStopSlave(self):
        self.queueResponse(None)
        self.client.stopSlave('dummy-slaveId')

        command = self.getCommand()
        self.checkValue(command, 'action', 'stopSlave')
        self.checkValue(command, 'slaveId', 'dummy-slaveId')
        self.checkValue(command, 'delayed', True)

        self.queueResponse(None)
        self.client.stopSlave('dummy-slaveId', delayed = False)
        command = self.getCommand()
        self.checkValue(command, 'delayed', False)

    def testStopMaster(self):
        self.queueResponse(None)
        self.client.stopMaster('dummy-masterId')
        command = self.getCommand()
        self.checkValue(command, 'action', 'stopMaster')
        self.checkValue(command, 'masterId', 'dummy-masterId')
        self.checkValue(command, 'delayed', True)

        self.queueResponse(None)
        self.client.stopMaster('dummy-masterId', delayed = False)
        command = self.getCommand()
        self.checkValue(command, 'delayed', False)

    def testSetSlaveTTL(self):
        self.queueResponse(None)
        self.client.setSlaveTTL('dummy-slaveId', 60)
        command = self.getCommand()
        self.checkValue(command, 'action', 'setSlaveTTL')
        self.checkValue(command, 'slaveId', 'dummy-slaveId')
        self.checkValue(command, 'TTL', 60)

    def testSetSlaveLimit(self):
        self.queueResponse(None)
        self.client.setSlaveLimit('dummy-masterId', 3)
        command = self.getCommand()

        self.checkValue(command, 'action', 'setSlaveLimit')
        self.checkValue(command, 'masterId', 'dummy-masterId')
        self.checkValue(command, 'limit', 3)


if __name__ == "__main__":
    testsuite.main()
