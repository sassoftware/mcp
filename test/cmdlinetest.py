#!/usr/bin/python2.4
#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()

import mcp_helper

import simplejson

from mcp import cmdline, client, queue

from conary import versions
from conary import conaryclient
from conary.deps import deps


class CmdlnTest(mcp_helper.MCPTest):
    def testCmdLine(self):
        envArgs = ['-T', 'Test Suite', '-H', '127.0.0.1', '-P', '61613', '-t',
                   'RAW_FS_IMAGE', 'group-foo=test.rpath.local@rpl:1']
        class MockClient(object):
            class MockRepos(object):
                def findTrove(self, *args, **kwargs):
                    return (('dummy', versions.VersionFromString( \
                                '/test.rpath.local@rpl:1/3.0.0-1-1'),
                             deps.Flavor()),)
            def __init__(self, cfg):
                self.repos = self.MockRepos()
                self.cfg = cfg
            def getRepos(self):
                return self.repos

        self.statusIndex = 0
        def dummyStatus(*args, **kwargs):
            idx = self.statusIndex
            self.statusIndex += 1
            if idx == 3:
                raise AssertionError("to test a codepath")
            return (jobstatus.WAITING,
                    jobstatus.STARTING,
                    jobstatus.RUNNING,
                    jobstatus.RUNNING,
                    jobstatus.RUNNING,
                    jobstatus.BUILT)[idx], ''

        def ignoreInput(*args, **kwargs):
            return False, None

        def dummyVersion(*args, **kwargs):
            return False, '3.0.0'

        def dummyRead(*args, **kwargs):
            return simplejson.dumps({'urls' : (('', ''),),
                                     'jobId' : 'dummy-build-25'})

        jobStatus = client.MCPClient.jobStatus
        submitJob = client.MCPClient.submitJob
        stopJob = client.MCPClient.stopJob
        getJSVersion = client.MCPClient.getJSVersion
        read = queue.Queue.read
        ConaryClient = conaryclient.ConaryClient
        try:
            client.MCPClient.jobStatus = dummyStatus
            client.MCPClient.submitJob = ignoreInput
            client.MCPClient.stopJob = ignoreInput
            client.MCPClient.getJSVersion = dummyVersion
            queue.Queue.read = dummyRead
            conaryclient.ConaryClient = MockClient
            out, err = self.captureOutput(cmdline.main, envArgs)
        finally:
            conaryclient.ConaryClient = ConaryClient
            client.MCPClient.jobStatus = jobStatus
            client.MCPClient.submitJob = submitJob
            client.MCPClient.stopJob = stopJob
            client.MCPClient.getJSVersion = getJSVersion
            queue.Queue.read = read

            self.failIf('Total' not in out, "cmdline did not reach last stage")

if __name__ == "__main__":
    testsuite.main()
