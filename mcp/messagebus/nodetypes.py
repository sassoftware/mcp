#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

"""
Declarations of all bus node types used by the MCP.
"""

from rmake.lib.apiutils import freeze, thaw
from rmake.multinode import nodetypes


class DispatcherNodeType(nodetypes.NodeType):
    nodeType = 'image_dispatcher'


class MasterNodeType(nodetypes.NodeType):
    nodeType = 'image_master'

    def __init__(self, slots, jobs, machineInfo):
        self.slots = slots
        self.jobs = jobs
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
