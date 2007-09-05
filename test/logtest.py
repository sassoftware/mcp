#!/usr/bin/python2.4
#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()

import mcp_helper
import logging
import os
import tempfile

from mcp import mcp_log

from conary.lib import util

class LogTest(mcp_helper.MCPTest):
    def testLogHup(self):
        log = logging.getLogger('')
        handlers = log.handlers[:]
        # make a directory because we're going to delete the logfile
        # using mkstemp would not be multi-instance safe
        tmpDir = tempfile.mkdtemp()
        try:
            logFile = os.path.join(tmpDir, 'test.log')
            mcp_log.addRootLogger(filename = logFile, level = logging.DEBUG)
            log.debug('test')
            self.failIf(not os.path.exists(logFile), "logfile was not created")
            os.unlink(logFile)
            log.debug('test')
            self.failIf(os.path.exists(logFile), "logfile unexpectedly created")
            mcp_log.signalHandler()
            log.debug('test')
            self.failIf(not os.path.exists(logFile),
                    "logfile was not created after hup")
        finally:
            for h in log.handlers[:]:
                if h not in handlers:
                    log.handlers.remove(h)
            util.rmtree(tmpDir)

    def testStreamHandler(self):
        log = logging.getLogger('')
        handlers = log.handlers[:]
        mcp_log.addRootLogger()
        h = [x for x in log.handlers if x not in handlers]
        hup = max([isinstance(x, mcp_log.HuppableFileHandler) for x in h])
        self.failIf(hup, "Expected a StreamHandler to be created")


if __name__ == "__main__":
    testsuite.main()
