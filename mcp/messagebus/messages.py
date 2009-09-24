#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

"""
Declarations of all bus message types used by the MCP.
"""

from rmake.multinode.messages import *
from mcp.messagebus import nodetypes


# Commands
class ImageCommandMessage(Message):
    pass


class ResetCommand(ImageCommandMessage):
    messageType = 'image_command_reset'


# Events
class ImageEventMessage(Message):
    pass


class MasterStatusMessage(ImageEventMessage):
    messageType = 'image_master_status'

    def set(self, node):
        self.payload.node = node

    def payloadToDict(self):
        return dict(node=self.payload.node.freeze())

    def loadPayloadFromDict(self, d):
        self.payload.node = nodetypes.MasterNodeType.thaw(d['node'][1])
