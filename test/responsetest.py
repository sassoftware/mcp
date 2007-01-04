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

class ResponseTest(mcp_helper.MCPTest):
    def getSlaveResponse(self):
        if self.slaveResponse.response.connection.sent:
            return simplejson.loads( \
                self.slaveResponse.response.connection.sent.pop()[1])
        else:
            self.fail("No response was sent")

    def getMasterResponse(self):
        if self.masterResponse.response.connection.sent:
            return simplejson.loads( \
                self.masterResponse.response.connection.sent.pop()[1])
        else:
            self.fail("No response was sent")

    def testBasicAttributes(self):
        assert self.slaveResponse.response.connectionName == \
            '/topic/test/response'
        assert self.masterResponse.response.connectionName == \
            '/topic/test/response'

    def testJobStatus(self):
        self.slaveResponse.jobStatus('dummy-jobId', 'running', 'huzzah')
        resp = self.getSlaveResponse()
        self.checkValue(resp, 'event', 'jobStatus')
        self.checkValue(resp, 'status', 'running')
        self.checkValue(resp, 'node', 'master:slave')
        self.checkValue(resp, 'jobId', 'dummy-jobId')
        self.checkValue(resp, 'statusMessage', 'huzzah')

    def testSlaveStatus(self):
        self.masterResponse.slaveStatus('dummy-slaveId', 'running',
                                        '2.0.2-5-12:x86')
        resp = self.getMasterResponse()
        self.checkValue(resp, 'event', 'slaveStatus')
        self.checkValue(resp, 'node', 'master')
        self.checkValue(resp, 'slaveId', 'dummy-slaveId')
        self.checkValue(resp, 'type', '2.0.2-5-12:x86')
        self.checkValue(resp, 'status', 'running')

    def testMasterStatus(self):
         self.masterResponse.masterStatus('x86', 2, [])
         resp = self.getMasterResponse()
         self.checkValue(resp, 'event', 'masterStatus')
         self.checkValue(resp, 'limit', 2)
         self.checkValue(resp, 'arch', 'x86')
         self.checkValue(resp, 'slaves', [])
         self.checkValue(resp, 'node', 'master')

    def testMasterOffline(self):
        self.masterResponse.masterOffline()
        resp = self.getMasterResponse()
        self.checkValue(resp, 'event', 'masterOffline')
        self.checkValue(resp, 'node', 'master')

    def testJobLog(self):
        self.slaveResponse.jobLog('dummy-jobId', 'log emssage')
        resp = self.getSlaveResponse()
        self.checkValue(resp, 'event', 'jobLog')
        self.checkValue(resp, 'jobId', 'dummy-jobId')
        self.checkValue(resp, 'message', 'log emssage')

    def testProtocol(self):
        self.masterResponse.protocol(2)
        resp = self.getMasterResponse()
        self.checkValue(resp, 'event', 'protocol')
        self.checkValue(resp, 'protocolVersion', 2)

        # this needs to be tested at least twice to ensure it's having an effect
        # protocolVersion is manipulated directly by the response object.
        self.masterResponse.protocol(3)
        resp = self.getMasterResponse()
        self.checkValue(resp, 'event', 'protocol')
        self.checkValue(resp, 'protocolVersion', 3)


if __name__ == "__main__":
    testsuite.main()
