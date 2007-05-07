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

archivePath = None
testPath = None

conaryDir = None
_setupPath = None
def setup():
    global _setupPath
    if _setupPath:
        return _setupPath
    global testPath
    global archivePath

    # set default CONARY_POLICY_PATH is it was not set.
    conaryPolicy = os.getenv('CONARY_POLICY_PATH', '/usr/lib/conary/policy')
    os.environ['CONARY_POLICY_PATH'] = conaryPolicy

    # set default MCP_PATH, if it was not set.
    parDir = '/'.join(os.path.realpath(__file__).split('/')[:-2])
    mcpPath = os.getenv('MCP_PATH', parDir)
    os.environ['MCP_PATH'] = mcpPath

    if mcpPath not in sys.path:
        sys.path.insert(0, mcpPath)
    # end setting default MCP_PATH/CONARY_POLICY_PATH

    if not os.environ.has_key('CONARY_PATH'):
        print "please set CONARY_PATH"
        sys.exit(1)
    paths = (os.environ['MCP_PATH'], os.environ['MCP_PATH'] + '/test',
             os.environ['CONARY_PATH'],
             os.path.normpath(os.environ['CONARY_PATH'] + "/../rmake"),
             os.path.normpath(os.environ['CONARY_PATH'] + "/../conary-test"),)
    pythonPath = os.getenv('PYTHONPATH') or ""
    for p in reversed(paths):
        if p in sys.path:
            sys.path.remove(p)
        sys.path.insert(0, p)
    for p in paths:
        if p not in pythonPath:
            pythonPath = os.pathsep.join((pythonPath, p))
    os.environ['PYTHONPATH'] = pythonPath

    if isIndividual():
        serverDir = '/tmp/conary-server'
        if os.path.exists(serverDir) and not os.path.access(serverDir, os.W_OK):
            serverDir = serverDir + '-' + pwd.getpwuid(os.getuid())[0]
        os.environ['SERVER_FILE_PATH'] = serverDir
    import testhelp
    testPath = testhelp.getTestPath()
    archivePath = testPath + '/' + "archive"
    parent = os.path.dirname(testPath)

    global conaryDir
    conaryDir = os.environ['CONARY_PATH']

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
