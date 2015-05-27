#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os

from conary import conarycfg
from conary.lib import cfgtypes

CONFIG_PATH = '/srv/rbuilder/mcp/config'


class CfgLogLevel(cfgtypes.CfgEnum):
    validValues = ['ERROR', 'WARNING', 'INFO', 'DEBUG']
    def checkEntry(self, val):
        cfgtypes.CfgEnum.checkEntry(self, val.upper())


class MCPConfig(conarycfg.ConfigFile):
    basePath = '/srv/rbuilder/mcp'
    logPath = '/var/log/rbuilder/mcp.log'
    logLevel = (CfgLogLevel, 'INFO')

    queueHost = '127.0.0.1'
    queuePort = (cfgtypes.CfgInt, 50900)

    # DEPRECATED
    namespace = None
    slaveTroveName = None
    slaveSetVersion = None
    slaveSetLabel = None
