#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import os
import time
import weakref
from rmake.lib import apirpc
from rmake.lib import server
from rmake.lib.apiutils import api, api_parameters, api_return, freeze, thaw, register
from mcp import image_job
from mcp.messagebus import bus_node
from mcp.messagebus import messages
from mcp.messagebus import nodetypes


class Dispatcher(bus_node.BusNode):
    """
    Message bus interface to the scheduler mechanism.
    """
    nodeType = nodetypes.DispatcherNodeType
    sessionClass = 'image_dispatcher'
    subscriptions = [
            '/register?nodeType=image_master',
            '/internal/nodes',
            '/image_event',
            ]

    def __init__(self):
        bus_node.BusNode.__init__(self, ('localhost', 50900))
        self.scheduler = Scheduler(self)

    # Node client machinery and entry points
    def onStart(self):
        # Reset all master nodes on dispatcher startup.
        msg = messages.ResetCommand()
        self.bus.sendMessage('/image_command', msg)

    def onTimer(self):
        # Cull dead nodes on a regular basis.
        self.scheduler.cull_nodes()

    def handleRegisterNodeMessage(self, msg):
        if not isinstance(msg.payload.node, nodetypes.MasterNodeType):
            return
        self.scheduler.set_node_info(msg.headers.sessionId, msg.payload.node)

    def handleNodeStatus(self, msg):
        if msg.headers.status == 'DISCONNECTED':
            self.scheduler.remove_node(msg.headers.statusId)

    def handleMasterStatusMessage(self, msg):
        self.scheduler.set_node_info(msg.headers.sessionId, msg.payload.node)

    # API server machinery and entry points
    @api(version=1)
    @api_parameters(1, 'ImageJob')
    @api_return(1, None)
    def add_job(self, callData, imageJob):
        self.scheduler.add_job(imageJob)


class Scheduler(object):
    """
    Assigns jobs to nodes.
    """
    def __init__(self, dispatcher):
        self.dispatcher = weakref.ref(dispatcher)
        self.nodes = {}
        self.queued_jobs = []
        self._logger = dispatcher._logger

    def set_node_info(self, session_id, node_info):
        if session_id in self.nodes:
            self.nodes[session_id].set_info(node_info)
        else:
            self._logger.info("Added node %s to pool.", session_id)
            self.nodes[session_id] = SchedulerNode(node_info)
        self.assign_jobs()

    def remove_node(self, session_id):
        """
        Remove the given node and mark all its jobs as failed.
        """
        if session_id not in self.nodes:
            return
        node = self.nodes[session_id]

        # Any jobs still assigned to the node fail.
        for job in node.assigned_jobs:
            self.fail_job(job.uuid)

        del self.nodes[session_id]
        self._logger.info("Removed node %s from pool.", session_id)

    def cull_nodes(self):
        """
        Delete nodes that haven't been seen in a few heartbeats.
        """
        for session_id, node in self.nodes.items():
            if not node.is_alive():
                self._logger.info("No heartbeat from node %s; "
                        "removing from pool.", session_id)
                self.remove_node(session_id)

    def assign_jobs(self):
        """
        Attempt to assign queued jobs to a node.
        """

    def add_job(self, image_job):
        """
        Add a new job to the queue.
        """
        job = SchedulerJob(image_job)
        self.queued_jobs.append(job)
        self.assign_jobs()
        return job.uuid

    def fail_job(self, job_uuid):
        pass


class SchedulerJob(object):
    """
    Container for information the dispatcher has about a job.
    """
    def __init__(self, image_job):
        self.image_job = image_job
        self.uuid = os.urandom(16).encode('hex')


class SchedulerNode(object):
    """
    Container for information the dispatcher has about a node.
    """
    def __init__(self, node_info):
        self.last_seen = time.time()
        self.node_info = node_info
        self.assigned_jobs = []

    def is_alive(self):
        # 16 seconds is 3 heartbeat intervals + 1
        return time.time() - self.last_seen < 16

    def set_info(self, node_info):
        self.last_seen = time.time()
        self.node_info = node_info


if __name__ == '__main__':
    Dispatcher().serve_forever()
