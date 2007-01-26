#
# Copyright (c) 2005-2006 rPath, Inc.
#
# All rights reserved
#

import os

from conary import conarycfg
from conary.lib import cfgtypes

class MCPConfig(conarycfg.ConfigFile):
    basePath = os.path.join(os.path.sep, 'srv', 'rbuilder', 'mcp')
    logPath = os.path.join(basePath, 'log')

    queueHost = '127.0.0.1'
    queuePort = (cfgtypes.CfgInt, 61613)
    namespace = 'mcp'

    slaveTroveName = 'group-jobslave'
    slaveTroveLabel = 'jobslave.rb.rpath.com@rpl:devel'
