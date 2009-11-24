#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import logging
import optparse
import os
import time
import weakref
from mcp import config
from mcp import image_job # import so it gets registered
from mcp.messagebus import bus_node
from mcp.messagebus import messages
from mcp.messagebus import nodetypes
from mcp.util import setupLogging
from os import _exit
from rmake.lib.apiutils import (api, api_parameters, api_return,
        freeze, thaw, register)

log = logging.getLogger(__name__)


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

    def __init__(self, host='localhost', port=50900):
        bus_node.BusNode.__init__(self, (host, port))
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
        if not isinstance(msg.getNode(), nodetypes.MasterNodeType):
            return
        self.scheduler.update_node(msg.getSessionId(), msg.getNode())

    def handleNodeStatus(self, msg):
        if msg.getStatus() == 'DISCONNECTED':
            self.scheduler.remove_node(msg.getStatusId())

    def handleMasterStatusMessage(self, msg):
        self.scheduler.update_node(msg.getSessionId(), msg.getNode())

    def handleJobCompleteMessage(self, msg):
        self.scheduler.remove_job(msg.getUUID())

    # API server machinery and entry points
    @api(version=1)
    @api_parameters(1)
    @api_return(1, 'ImageJobs')
    def list_jobs(self, callData):
        """Return a list of all known job UUIDs."""
        return self.scheduler.list_jobs()

    @api(version=1)
    @api_parameters(1)
    @api_return(1, 'ImageJobs')
    def list_queued_jobs(self, callData):
        """Return a list of queued job objects."""
        return self.scheduler.queued_jobs

    @api(version=1)
    @api_parameters(1)
    @api_return(1, 'ImageNodes')
    def list_nodes(self, callData):
        """Return a list of all known nodes."""
        return self.scheduler.nodes.values()

    @api(version=1)
    @api_parameters(1, 'ImageJob')
    @api_return(1, None)
    def add_job(self, callData, imageJob):
        return self.scheduler.add_job(imageJob)

    @api(version=1)
    @api_parameters(1, None)
    @api_return(1, None)
    def stop_job(self, callData, uuid):
        self.scheduler.stop_job(uuid)


class Scheduler(object):
    """
    Assigns jobs to nodes.
    """
    def __init__(self, dispatcher):
        self.dispatcher = weakref.ref(dispatcher)

        self.nodes = {}
        """List of nodes and what jobs they are running."""

        self.queued_jobs = []
        """List of jobs to start when a slot becomes free."""

    # Nodes
    def update_node(self, session_id, node):
        if session_id not in self.nodes:
            self.nodes[session_id] = ImageNode(session_id)
            log.info("Added node %s to pool.", session_id)
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
            log.info("Job %s failed due to node %s shutdown.",
                    job_uuid, session_id)
            # TODO: notify

        del self.nodes[session_id]
        log.info("Removed node %s from pool.", session_id)

    def cull_nodes(self):
        """
        Delete nodes that haven't been seen in a few heartbeats.
        """
        for session_id, node in self.nodes.items():
            if not node.is_alive():
                log.info("No heartbeat from node %s; "
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
    def list_jobs(self):
        """
        Return a list of all job UUIDs known to this dispatcher, running and
        queued.
        """
        jobs = set(self.queued_jobs)
        for node in self.nodes.values():
            jobs.update(node.jobs)
        return jobs

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
            self.queued_jobs.pop(0)

    def add_job(self, job):
        """
        Add a new job to the queue and try to assign it to a node.
        """
        job.assign_uuid()
        self.queued_jobs.append(job)
        log.info("Added new job %s from %s", job.uuid, job.rbuilder_url)
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
        log.info("Sent job %s to node %s.", job.uuid, node.session_id)
        node.add_job(job)

    def remove_job(self, job_uuid):
        """Remove a job and return the node that was running it, or None."""
        for node in self.nodes.values():
            job = node.find_job(job_uuid)
            if not job:
                continue

            log.info("Removing job %s", job_uuid)
            node.remove_job(job)
            self.assign_jobs()
            return node

        return None

    def stop_job(self, job_uuid):
        """Terminate a queued or running job."""

        # Remove from the queue
        for n, job in enumerate(self.queued_jobs):
            if job.uuid == job_uuid:
                log.info("Stopped queued job %s.", job_uuid)
                self.queued_jobs.remove(job)
                return

        # Remove from running nodes
        node = self.remove_job(job_uuid)
        if not node:
            log.info("Ignored request to stop unknown job %s.", job_uuid)
            return

        msg = messages.StopCommand()
        msg.set(job_uuid)
        self.dispatcher().bus.sendMessage('/image_command', msg,
                node.session_id)
        log.info("Stopped running job %s on node %s.", job_uuid,
                node.session_id)


class ImageNode(object):
    def __init__(self, session_id):
        self.session_id = session_id
        self.jobs = set()
        self.slots = None
        self.machine_info = None
        self.first_seen = None
        self.last_seen = None

    def update(self, slots, machine_info):
        self.slots = slots
        self.machine_info = machine_info
        self.last_seen = time.time()
        if not self.first_seen:
            self.first_seen = self.last_seen

    def __repr__(self):
        return '<ImageNode %s>' % (self.session_id,)

    def __freeze__(self):
        return {
                'session_id': self.session_id,
                'jobs': freeze('ImageJobs', self.jobs),
                'slots': self.slots,
                'machine_info': freeze('MachineInformation',
                    self.machine_info),
                }

    @classmethod
    def __thaw__(cls, d):
        self = cls(d['session_id'])
        self.jobs = set(thaw('ImageJobs', d['jobs']))
        self.slots = d['slots']
        self.machine_info = thaw('MachineInformation', d['machine_info'])
        return self

    def add_job(self, job):
        self.jobs.add(job)

    def remove_job(self, job):
        self.jobs.remove(job)

    def find_job(self, uuid):
        for job in self.jobs:
            if job.uuid == uuid:
                return job
        return None

    def is_alive(self):
        # 16 seconds = 3 heartbeats + 1
        return time.time() - self.last_seen < 16

    def has_slots(self):
        return len(self.jobs) < self.slots

    def get_score(self):
        return len(self.jobs) / float(self.slots)
register(ImageNode)


def main(args):
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config-file', default=config.CONFIG_PATH)
    parser.add_option('-n', '--no-daemon', action='store_true')
    parser.add_option('-p', '--pid-file', default='/var/run/mcp-dispatcher.pid')
    options, args = parser.parse_args(args)

    cfg = config.MCPConfig()
    if (options.config_file != config.CONFIG_PATH
            or os.path.exists(config.CONFIG_PATH)):
        cfg.read(options.config_file)

    setupLogging(cfg.logLevel, toFile=cfg.logPath, toStderr=options.no_daemon)

    dispatcher = Dispatcher(cfg.queueHost, cfg.queuePort)

    if options.no_daemon:
        dispatcher.serve_forever()
    else:
        # Double-fork to daemonize
        pid = os.fork()
        if pid:
            return

        pid = os.fork()
        if pid:
            _exit(0)

        try:
            os.setsid()
            devNull = os.open(os.devnull, os.O_RDWR)
            os.dup2(devNull, 0)
            os.dup2(devNull, 1)
            os.dup2(devNull, 2)
            os.close(devNull)

            pid = open(options.pid_file, 'w')
            pid.write(str(os.getpid()))
            pid.close()

            dispatcher.serve_forever()

        finally:
            try:
                os.unlink(options.pid_file)
            finally:
                _exit(0)
