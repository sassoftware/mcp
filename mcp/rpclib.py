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
Tools for performing RPC against nodes on the message bus.
"""
from conary.lib.util import rethrow
#from rmake.lib.apiutils import register, freeze, thaw
from rmake.errors import OpenError
from rmake.messagebus import rpclib
from rmake.multinode import nodetypes
from rmake.multinode.server import messagebus
from mcp import dispatcher
from mcp.mcp_error import BuildSystemUnreachableError
from mcp.messagebus import nodetypes as mcp_nodetypes


def findBusNode(busClient, sessionClass):
    """
    Return the sessionId of a node on the message bus with the given
    C{sessionClass}, or C{None} if no node with that class is connected.
    """
    busProxy = messagebus.MessageBusRPCClient(busClient)
    for nodeId, nodeClass in sorted(busProxy.listSessions().items()):
        if nodeClass == sessionClass:
            return nodeId
    return None


class DispatcherRPCClient(rpclib.SessionProxy):
    def __init__(self, busClient, dispatcherId=None):
        if dispatcherId is None:
            try:
                dispatcherId = findBusNode(busClient,
                        dispatcher.Dispatcher.sessionClass)
            except OpenError, err:
                rethrow(BuildSystemUnreachableError)

            if dispatcherId is None:
                raise BuildSystemUnreachableError(
                        "Could not contact the dispatcher")

        rpclib.SessionProxy.__init__(self, dispatcher.Dispatcher, busClient,
                dispatcherId)
