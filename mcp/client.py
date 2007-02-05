#
# Copyright (c) 2005-2006 rPath, Inc.
#
# All rights reserved
#

import sys
import simplejson
from random import SystemRandom
random = SystemRandom()
import md5

from mcp import queue
from mcp import mcp_error

from conary import conarycfg
from conary.lib import cfgtypes

class MCPClientConfig(conarycfg.ConfigFile):
    queueHost = '127.0.0.1'
    queuePort = (cfgtypes.CfgInt, 61613)
    namespace = 'mcp'

class MCPClient(object):
    def __init__(self, cfg):
        m = md5.new()
        m.update(str(random.randint(0, 2 ** 128)))
        self.uuid = m.hexdigest()
        self.cfg = cfg
        self.command = queue.Queue(cfg.queueHost, cfg.queuePort,
                                   'command', namespace = cfg.namespace,
                                   autoSubscribe = False)
        self.response = queue.Topic(cfg.queueHost, cfg.queuePort, self.uuid,
                                    namespace = cfg.namespace, timeOut = None)

    def __del__(self):
        self.command.disconnect()
        self.response.disconnect()

    def _send(self, **data):
        data['uuid'] = self.uuid
        data['protocolVersion'] = 1

        action = sys._getframe(1).f_code.co_name
        assert action in self.__class__.__dict__
        if action.startswith('_'):
            raise mcp_error.ProtocolError('Illegal Action: %s' % action)
        data['action'] = sys._getframe(1).f_code.co_name

        self.command.send(simplejson.dumps(data))
        res = self.response.read()
        if res:
            error, res = simplejson.loads(res)
            if error:
                exc, e = res
                if exc in mcp_error.__dict__:
                    raise mcp_error.__dict__[exc](e)
                else:
                    raise Exception(str(exc), str(e))
            return res

    def submitJob(self, job):
        return self._send(data = job)

    def stopJob(self, jobId):
        return self._send(jobId = jobId)

    def jobStatus(self, jobId):
        return self._send(jobId = jobId)

    def slaveStatus(self):
        return self._send()

    def stopSlave(self, slaveId, delayed = True):
        return self._send(slaveId = slaveId, delayed = delayed)

    def stopMaster(self, masterId, delayed = True):
        return self._send(masterId = masterId, delayed = delayed)

    def setSlaveTTL(self, slaveId, TTL):
        return self._send(slaveId = slaveId, TTL = TTL)

    def setSlaveLimit(self, masterId, limit):
        return self._send(masterId = masterId, limit = limit)

    def clearCache(self, masterId):
        return self._send(masterId = masterId)

    def getJSVersion(self):
        return self._send()


if __name__ == '__main__':
    import bdb
    def run():
        cfg = MCPClientConfig()
        client = MCPClient(cfg)
        import epdb
        epdb.st()
    try:
        run()
    except bdb.BdbQuit:
        pass
