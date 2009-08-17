#!/usr/bin/python
# -*- mode: python -*-
#
# Copyright (c) 2004-2009 rPath, Inc.
#

import sys
import unittest

EXCLUDED_PATHS = ['test', 'setup.py']


def setup():
    from testrunner import pathManager
    mcpPath = pathManager.addExecPath('MCP_PATH')
    pathManager.addResourcePath('TEST_PATH',path=mcpPath)
    conaryPath = pathManager.addExecPath('CONARY_PATH')
    stompPath = pathManager.addExecPath('STOMP_PATH')

    from conary.lib import util
    sys.excepthook = util.genExcepthook(True)

    # if we're running with COVERAGE_DIR, we'll start covering now
    from conary.lib import coveragehook

    # import tools normally expected in findTrove.
    from testrunner.testhelp import context, TestCase, findPorts, SkipTestException
    sys.modules[__name__].context = context
    sys.modules[__name__].TestCase = TestCase
    sys.modules[__name__].findPorts = findPorts
    sys.modules[__name__].SkipTestException = SkipTestException

    # MCP specific tweaks
    from mcp_test import mcp_helper
    import stomp
    stomp.Connection = mcp_helper.DummyConnection
    # end MCP specific tweaks


def main(argv=None, individual=True):
    from testrunner import testhelp,pathManager
    class rBuilderTestSuiteHandler(testhelp.TestSuiteHandler):
        suiteClass = testhelp.ConaryTestSuite

        def getCoverageDirs(self, environ):
            return pathManager.getPath('MCP_PATH')

        def getCoverageExclusions(self, environ):
            return EXCLUDED_PATHS

    root = pathManager.getPath('MCP_PATH')
    handler = rBuilderTestSuiteHandler(individual=individual)

    if argv is None:
        argv = list(sys.argv)
    results = handler.main(argv)
    return results.getExitCode()


if __name__ == '__main__':
    setup()
    sys.exit(main(sys.argv, individual=False))
