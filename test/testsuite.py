#!/usr/bin/python2.4
# -*- mode: python -*-
#
# Copyright (c) 2004-2006 rPath, Inc.
#

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

testPath = None

conaryDir = None
_setupPath = None
def setup():
    global _setupPath
    if _setupPath:
        return _setupPath
    global testPath

    if not os.environ.has_key('CONARY_PATH'):
        print "please set CONARY_PATH"
        sys.exit(1)

    conaryPath      = os.getenv('CONARY_PATH')
    conaryTestPath  = os.getenv('CONARY_TEST_PATH', os.path.join(conaryPath, '..', 'conary-test'))
    mcpPath         = os.getenv('MCP_PATH',         '..')
    mcpTestPath     = os.getenv('MCP_TEST_PATH',    '.')

    sys.path = [os.path.realpath(x) for x in (mcpPath, mcpTestPath, conaryPath, conaryTestPath)] + sys.path
    os.environ.update(dict(CONARY_PATH=conaryPath, CONARY_TEST_PATH=conaryTestPath,
        MCP_PATH=mcpPath, MCP_TEST_PATH=mcpTestPath, PYTHONPATH=(':'.join(sys.path))))

    import testhelp
    testPath = testhelp.getTestPath()

    global conaryDir
    conaryDir = conaryPath

    from conary.lib import util
    sys.excepthook = util.genExcepthook(True)

    # if we're running with COVERAGE_DIR, we'll start covering now
    from conary.lib import coveragehook

    # import tools normally expected in findTrove.
    from testhelp import context, TestCase, findPorts, SkipTestException
    sys.modules[__name__].context = context
    sys.modules[__name__].TestCase = TestCase
    sys.modules[__name__].findPorts = findPorts
    sys.modules[__name__].SkipTestException = SkipTestException

    # MCP specific tweaks
    import mcp_helper
    import stomp
    stomp.Connection = mcp_helper.DummyConnection
    # end MCP specific tweaks

    _setupPath = testPath
    return testPath

_individual = False

def isIndividual():
    global _individual
    return _individual


EXCLUDED_PATHS = ['test', 'setup.py']

def main(argv=None, individual=True):
    import testhelp
    testhelp.isIndividual = isIndividual
    class rBuilderTestSuiteHandler(testhelp.TestSuiteHandler):
        suiteClass = testhelp.ConaryTestSuite

        def getCoverageDirs(self, environ):
            return environ['mcp']

        def getCoverageExclusions(self, environ):
            return EXCLUDED_PATHS

    global _handler
    global _individual
    _individual = individual
    if argv is None:
        argv = list(sys.argv)
    topdir = testhelp.getTestPath()
    cwd = os.getcwd()
    if topdir not in sys.path:
        sys.path.insert(0, topdir)
    if cwd != topdir and cwd not in sys.path:
        sys.path.insert(0, cwd)

    handler = rBuilderTestSuiteHandler(individual=individual, topdir=topdir,
                                       testPath=testPath, conaryDir=conaryDir)
    _handler = handler
    results = handler.main(argv)
    return (not results.wasSuccessful())

if __name__ == '__main__':
    setup()
    sys.exit(main(sys.argv, individual=False))
