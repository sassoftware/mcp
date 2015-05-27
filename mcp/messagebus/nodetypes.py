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


"""
Declarations of all bus node types used by the MCP.
"""

import time
from rmake.lib.apiutils import freeze, thaw
from rmake.multinode import nodetypes


class DispatcherNodeType(nodetypes.NodeType):
    nodeType = 'image_dispatcher'


class MasterNodeType(nodetypes.NodeType):
    nodeType = 'image_master'

    def __init__(self, slots, machineInfo):
        self.slots = slots
        self.machineInfo = machineInfo

    def freeze(self):
        d = self.__dict__.copy()
        d['machineInfo'] = freeze('MachineInformation', self.machineInfo)
        return self.nodeType, d

    @classmethod
    def thaw(cls, d):
        self = cls(**d)
        self.machineInfo = thaw('MachineInformation', self.machineInfo)
        return self
