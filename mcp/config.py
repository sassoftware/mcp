#
# Copyright (c) 2005-2009 rPath, Inc.
#
# All rights reserved.
#

import os

from conary import conarycfg
from conary.lib import cfgtypes


class CfgLogLevel(cfgtypes.CfgEnum):
    validValues = ['ERROR', 'WARNING', 'INFO', 'DEBUG']
    def checkEntry(self, val):
        cfgtypes.CfgEnum.checkEntry(self, val.upper())


class MCPConfig(conarycfg.ConfigFile):
    basePath = '/srv/rbuilder/mcp'
    logPath = '/var/log/rbuilder'
    logLevel = (CfgLogLevel, 'INFO')

    queueHost = '127.0.0.1'
    queuePort = (cfgtypes.CfgInt, 50900)

    # DEPRECATED
    namespace = None
    slaveTroveName = None
    slaveSetVersion = None
    slaveSetLabel = None
