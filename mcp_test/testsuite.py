#!/usr/bin/python
# -*- mode: python -*-
#
# Copyright (c) 2004-2006 rPath, Inc.
#
import bootstrap

import bdb
import cPickle
import grp
import sys
import os
import os.path
import pwd
import socket
import re
import tempfile
import time
import types
import unittest
import __builtin__

_setupPath = None
def setup():
    global _setupPath
    if _setupPath:
        return _setupPath

    from testrunner import pathManager
    mcpPath = pathManager.addExecPath('MCP_PATH')
    mcpTestPath = pathManager.addExecPath('MCP_TEST_PATH')
    pathManager.addResourcePath('TEST_PATH',path=mcpTestPath)
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
    import mcp_helper
    import stomp
    stomp.Connection = mcp_helper.DummyConnection
    # end MCP specific tweaks

    _setupPath = pathManager.getPath('MCP_TEST_PATH')
    return _setupPath

_individual = False

def isIndividual():
    global _individual
    return _individual


EXCLUDED_PATHS = ['test', 'setup.py']

def main(argv=None, individual=True):
    from testrunner import testhelp,pathManager
    testhelp.isIndividual = isIndividual
    class rBuilderTestSuiteHandler(testhelp.TestSuiteHandler):
        suiteClass = testhelp.ConaryTestSuite

        def getCoverageDirs(self, environ):
            return pathManager.getPath('MCP_PATH')

        def getCoverageExclusions(self, environ):
            return EXCLUDED_PATHS

    global _handler
    global _individual
    _individual = individual

    handler = rBuilderTestSuiteHandler(individual=individual, topdir=pathManager.getPath('MCP_TEST_PATH'),
                                       testPath=pathManager.getPath('MCP_TEST_PATH'),
                                       conaryDir=pathManager.getPath('CONARY_PATH'))
    _handler = handler

    if argv is None:
        argv = list(sys.argv)
    results = handler.main(argv)
    return (not results.wasSuccessful())

if __name__ == '__main__':
    setup()
    sys.exit(main(sys.argv, individual=False))
