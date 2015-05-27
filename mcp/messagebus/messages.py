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
Declarations of all bus message types used by the MCP.
"""

from rmake.lib.apiutils import freeze, thaw
from rmake.multinode.messages import *
from mcp.messagebus import nodetypes


# Commands
class ImageCommandMessage(Message):
    pass


class ResetCommand(ImageCommandMessage):
    messageType = 'image_command_reset'


class JobCommand(ImageCommandMessage):
    messageType = 'image_command_job'

    def set(self, job):
        self.payload.job = job

    def payloadToDict(self):
        return dict(job=freeze('ImageJob', self.payload.job))

    def loadPayloadFromDict(self, d):
        self.set(thaw('ImageJob', d['job']))


class StopCommand(ImageCommandMessage):
    messageType = 'image_command_stop'

    def set(self, uuid):
        self.headers.uuid = uuid

    def getUUID(self):
        return self.headers.uuid


class SetSlotsCommand(ImageCommandMessage):
    messageType = 'image_command_set_slots'

    def set(self, slots):
        self.headers.slots = slots

    def getSlots(self):
        return self.headers.slots


# Events
class ImageEventMessage(Message):
    pass


class MasterStatusMessage(ImageEventMessage):
    messageType = 'image_master_status'

    def set(self, node):
        self.payload.node = node

    def getNode(self):
        return self.payload.node

    def payloadToDict(self):
        return dict(node=self.payload.node.freeze())

    def loadPayloadFromDict(self, d):
        self.payload.node = nodetypes.MasterNodeType.thaw(d['node'][1])


class JobCompleteMessage(ImageEventMessage):
    messageType = 'image_job_complete'

    def set(self, uuid):
        self.headers.uuid = uuid

    def getUUID(self):
        return self.headers.uuid
