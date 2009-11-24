#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#


from mcp import image_job
from mcp import rpclib
from rmake.lib.procutil import getNetName
from rmake.messagebus import busclient


class Client(object):
    # Pass this to new_job() for rbuilder_url and the local system's primary IP
    # address will be used.
    LOCAL_RBUILDER = -1
    _local_address = None

    def __init__(self, busHost='127.0.0.1', busPort=50900):
        self.bus = busclient.MessageBusClient(busHost, busPort, None)
        self.bus.logger.setQuietMode()
        self.dispatcher = rpclib.DispatcherRPCClient(self.bus)

    def get_jobs(self):
        return self.dispatcher.list_jobs()

    def list_jobs(self):
        return [x.uuid for x in self.dispatcher.list_jobs()]

    def list_queued_jobs(self):
        return self.dispatcher.list_queued_jobs()

    def list_nodes(self):
        return self.dispatcher.list_nodes()

    def new_job(self, rbuilder_url, job_data):
        if rbuilder_url == self.LOCAL_RBUILDER:
            if not self._local_address:
                self.__class__._local_address = getNetName()
            rbuilder_url = 'http://[%s]/' % self._local_address
        job = image_job.ImageJob(rbuilder_url, job_data)
        return self.add_job(job)

    def add_job(self, job):
        return self.dispatcher.add_job(job)

    def stop_job(self, uuid):
        return self.dispatcher.stop_job(uuid)
