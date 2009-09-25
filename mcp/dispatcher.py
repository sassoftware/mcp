#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import collections
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
        self.scheduler.update_node(msg.headers.sessionId, msg.payload.node)

    def handleNodeStatus(self, msg):
        if msg.headers.status == 'DISCONNECTED':
            self.scheduler.remove_node(msg.headers.statusId)

    def handleMasterStatusMessage(self, msg):
        self.scheduler.update_node(msg.headers.sessionId, msg.payload.node)

    # API server machinery and entry points
    @api(version=1)
    @api_parameters(1, 'ImageJob')
    @api_return(1, None)
    def add_job(self, callData, imageJob):
        return self.scheduler.add_job(imageJob)


class Scheduler(object):
    """
    Assigns jobs to nodes.
    """
    def __init__(self, dispatcher):
        self.dispatcher = weakref.ref(dispatcher)
        self.nodes = {}
        self.queued_jobs = collections.deque()
        self._logger = dispatcher._logger

    # Nodes
    def update_node(self, session_id, node):
        if session_id not in self.nodes:
            self.nodes[session_id] = SchedulerNode(session_id)
            self._logger.info("Added node %s to pool.", session_id)
        self.nodes[session_id].update(node.slots, node.machineInfo)
        self.assign_jobs()

    def remove_node(self, session_id):
        """
        Remove the given node and mark all its jobs as failed.
        """
        if session_id not in self.nodes:
            return
        node = self.nodes[session_id]

        # Any jobs still assigned to the node fail.
        for job_uuid in node.jobs:
            self._logger.info("Job %s failed due to node %s shutdown.",
                    job_uuid, session_id)
            # TODO: notify

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

    def get_slot(self):
        """
        Find the node with the best slot for a new job.
        """
        open_nodes = [x for x in self.nodes.values() if x.has_slots()]
        if not open_nodes:
            return None
        return sorted(open_nodes, key=lambda x: x.get_score())[0]

    # Jobs
    def assign_jobs(self):
        """
        Attempt to assign queued jobs to a node.
        """
        while self.queued_jobs:
            job = self.queued_jobs[0]
            node = self.get_slot()
            if not node:
                break
            self.send_job(node, job)
            self.queued_jobs.popleft()

    def add_job(self, job):
        """
        Add a new job to the queue and try to assign it to a node.
        """
        job.assign_uuid()
        self.queued_jobs.append(job)
        self._logger.info("Added new job %s.", job.uuid)
        self.assign_jobs()
        return job.uuid

    def send_job(self, node, job):
        """
        Assign a job to a node and notify the node.
        """
        msg = messages.JobCommand()
        msg.set(job)
        self.dispatcher().bus.sendMessage('/image_command', msg,
                node.session_id)
        self._logger.info("Sent job %s to node %s.", job.uuid, node.session_id)
        node.jobs.add(job.uuid)


class SchedulerNode(object):
    def __init__(self, session_id):
        self.session_id = session_id
        self.jobs = set()
        self.slots = None
        self.machine_info = None
        self.last_seen = None

    def update(self, slots, machine_info):
        self.slots = slots
        self.machine_info = machine_info
        self.last_seen = time.time()

    def is_alive(self):
        # 16 seconds = 3 heartbeats + 1
        return time.time() - self.last_seen < 16

    def has_slots(self):
        return len(self.jobs) < self.slots

    def get_score(self):
        return len(self.jobs) / float(self.slots)


if __name__ == '__main__':
    Dispatcher().serve_forever()
