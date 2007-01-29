#
# Copyright (c) 2005-2006 rPath, Inc.
#
# All rights reserved
#

import os, sys
import time
import threading
import simplejson

from conary import conaryclient
from conary.errors import TroveNotFound
from conary.deps import deps

from mcp import queue
from mcp import mcp_error
from mcp import config
import traceback

PROTOCOL_VERSION = 1

def getSuffix(frozenFlavor):
    flavors = ('x86_64', 'x86')
    flav = deps.ThawFlavor(str(frozenFlavor))
    for f in flavors:
        if flav.stronglySatisfies(deps.parseFlavor('is: ' + f)):
            return f
    return ''

def commandResponse(func):
    def wrapper(self, command):
        try:
            res = False, func(self, command)
        except Exception, e:
            if e.__class__.__name__ not in mcp_error.__dict__:
                exc, e, bt = sys.exc_info()
                print >> self.log, e.__class__.__name__, str(e)
                print >> self.log, '\n'.join(traceback.format_tb(bt))
                self.log.flush()
                e = mcp_error.InternalServerError()
            res = True, (e.__class__.__name__, str(e))
        if type(command) != dict:
            print >> self.log, "command is not a dict: %s" % str(command)
            self.log.flush()
        elif 'uuid' not in command:
            print >> self.log, "no response address: %s" % str(command)
            self.log.flush()
        else:
            self.responseTopic.send(command['uuid'], simplejson.dumps(res))
    return wrapper

def logErrors(func):
    def wrapper(self, command):
        try:
            func(self, command)
        except:
            exc, e, bt = sys.exc_info()
            print >> self.log, "Response Exception: (" + \
                e.__class__.__name__ + ')', str(e)
            print >> self.log, '\n'.join(traceback.format_tb(bt))
            self.log.flush()
    return wrapper

class MCPServer(object):
    def __init__(self, cfg):
        self.demandCounts = {}
        self.jobCounts = {}
        self.jobQueues = {}
        self.demand = {}
        self.jobSlaveCounts = {}
        self.jobSlaves = {}
        self.jobs = {}
        self.jobMasters = {}
        self.logFiles = {}

        self.cfg = cfg
        if cfg.logPath:
            self.log = open(os.path.join(cfg.logPath, 'mcp.log'), 'a+')
        else:
            self.log = sys.stderr
        # command queue is for sending commands *to* the mcp
        self.commandQueue = queue.Queue(cfg.queueHost, cfg.queuePort, 'command',
                                        namespace = cfg.namespace, timeOut = 0)
        # control topic is for mcp to send commands to all other units
        self.controlTopic = queue.Topic(cfg.queueHost, cfg.queuePort, 'control',
                                        namespace = cfg.namespace,
                                        autoSubscribe = False)
        # response Topic will be used both ways. other units talking to mcp
        # will use the channel "response" and mcp responding to clients will
        # send messages to a specific UUID
        self.responseTopic = queue.MultiplexedTopic(cfg.queueHost,
                                                    cfg.queuePort,
                                                    namespace = cfg.namespace,
                                                    timeOut = 0)
        self.responseTopic.addDest('response')
        # the channel
        self.postQueue = queue.MultiplexedQueue(cfg.queueHost, cfg.queuePort,
                                                autoSubscribe = False,
                                                timeOut = 0)

    def logJob(self, jobId, message):
        if jobId not in self.logFiles:
            if self.cfg.logPath:
                self.logFiles[jobId] = \
                    open(os.path.join( \
                        cfg.logPath,
                        jobId + time.strftime('-%Y-%m-%d_%H:%M:%S')), 'w')
                logFile = self.logFiles[jobId]
                logFile.write(message)
            else:
                print jobId + ':', message

    def getVersion(self, version):
        cc = conaryclient.ConaryClient()
        try:
            l = version and (self.cfg.slaveTroveLabel + '/' + version) \
                or self.cfg.slaveTroveLabel
            troves = cc.repos.findTrove( \
                None, (self.cfg.slaveTroveName, l, None))
        except TroveNotFound:
            return []
        troves = [x for x in troves \
                      if x[2].stronglySatisfies(deps.parseFlavor('xen,domU'))]
        if troves[0][2] is None:
            troves[0][2] == ''
        return troves[0]

    def getTrailingRevision(self, version):
        NVF = self.getVersion(version)
        return NVF and str(NVF[1].trailingRevision()) or ''

    def demandJobSlave(self, version, suffix, limit = 1):
        demand = '%s:%s' % (version, suffix)
        demandName = 'demand:%s' % suffix
        if demandName not in self.demand:
            self.demand[demandName] = \
                queue.Queue(self.cfg.queueHost, self.cfg.queuePort,
                            demandName, namespace = self.cfg.namespace,
                            autoSubscribe = False)
        count = self.demandCounts.get(demand, 0)
        if count < limit:
            data = {}
            data['protocolVersion'] = PROTOCOL_VERSION
            data['troveSpec'] = '%s=%s/%s' % (self.cfg.slaveTroveName,
                                              self.cfg.slaveTroveLabel, version)
            self.demand[demandName].send(simplejson.dumps(data))
            self.demandCounts[demand] = count + 1
            return True
        else:
            return False

    def addJobQueue(self, version, suffix):
        jobName = 'job%s:%s' % (version, suffix)
        self.jobQueues[jobName] = queue.Queue(self.cfg.queueHost,
                                         self.cfg.queuePort, jobName,
                                         namespace = self.cfg.namespace,
                                         autoSubscribe = False)
        self.demandJobSlave(version, suffix)

    def addJob(self, version, suffix, data):
        type = '%s:%s' % (version, suffix)
        queueName = 'job' + type
        if queueName not in self.jobQueues:
            self.addJobQueue(version, suffix)
        self.jobQueues[queueName].send(data)
        count = self.jobCounts.get(type, 0) + 1
        self.jobCounts[type] = count

    def handleJob(self, dataStr, force = False):
        try:
            data = simplejson.loads(dataStr)
        except:
            raise mcp_error.ProtocolError('unable to parse job')
        UUID = data['UUID']
        if (not force) and (UUID in self.jobs) and \
                (self.jobs[UUID]['status'][0] not in ('finished', 'failed')):
            raise mcp_error.JobConflict
        type = data['type']
        version = ''
        if type == 'build':
            version = \
                self.getTrailingRevision(data['data']['jsversion'])
            suffix = getSuffix(data['troveFlavor'])
        elif type == 'cook':
            version = self.getTrailingRevision(version = '')
            suffix = data['jobData']['arch']
        else:
            raise mcp_error.UnknownJobType('Unknown job type: %s' % type)
        self.jobs[data['UUID']] = \
            {'status' : ('waiting', 'Waiting to be processed'),
             'slaveId' : None,
             'data' : dataStr}
        self.addJob(version, suffix, dataStr)
        return data['UUID']

    def setSlaveTTL(self, slaveId, TTL):
        if slaveId not in self.jobSlaves:
            raise mcp_error.UnknownHost("Unknown Host: %s" % slaveId)
        control = {'protocolVersion' : PROTOCOL_VERSION,
                   'node' : slaveId,
                   'action' : 'setTTL',
                   'TTL' : TTL}
        self.controlTopic.send(simplejson.dumps(control))

    def stopSlave(self, slaveId, delayed):
        if slaveId not in self.jobSlaves:
            raise mcp_error.UnknownHost("Unknown Host: %s" % slaveId)
        if delayed:
            self.setSlaveTTL(slaveId, 0)
        else:
            # slave Id is masterId:slaveId so splitting on : gives masterId
            control = {'protocolVersion' : PROTOCOL_VERSION,
                       'node' : slaveId.split(':')[0],
                       'action' : 'stopSlave',
                       'slaveId' : slaveId}
            self.controlTopic.send(simplejson.dumps(control))

    def setSlaveLimit(self, masterId, limit):
        if masterId not in self.jobMasters:
            raise mcp_error.UnknownHost("Unknown Host: %s" % masterId)
        control = {'protocolVersion' : PROTOCOL_VERSION,
                   'node' : masterId,
                   'action' : 'slaveLimit',
                   'limit' : limit}
        self.controlTopic.send(simplejson.dumps(control))

    def stopJob(self, jobId):
        if jobId not in self.jobs:
            raise mcp_error.UnknownJob('Unknown Job ID: %s' % jobId)
        slaveId = self.jobs[jobId]['slaveId']
        if slaveId:
            # the slave only has one job running at a time, but the jobId
            # is included in the control packet to ensure a race condition can't
            # kill the wrong job.
            control = {'protocolVersion' : PROTOCOL_VERSION,
                       'node' : slaveId,
                       'action' : 'stopJob',
                       'jobId' : jobId}
            self.controlTopic.send(simplejson.dumps(control))

    def clearCache(self, masterId):
        if masterId not in self.jobMasters:
            raise mcp_error.UnknownHost("Unknown Host: %s" % masterId)
        control = {'protocolVersion' : PROTOCOL_VERSION,
                   'node' : masterId,
                   'action' : 'clearImageCache'}
        self.controlTopic.send(simplejson.dumps(control))

    @commandResponse
    def handleCommand(self, data):
        if data.get('protocolVersion') == 1:
            if data['action'] == 'submitJob':
                return self.handleJob(data['data'])
            elif data['action'] == 'slaveStatus':
                return self.jobMasters
            elif data['action'] == 'jobStatus':
                jobId = data.get('jobId')
                if jobId not in self.jobs:
                    raise mcp_error.UnknownJob('Unknown job Id: %s' % jobId)
                return self.jobs[data['jobId']]['status']
            elif data['action'] == 'stopMaster':
                masterId = data['masterId']
                if masterId not in self.jobMasters:
                    raise mcp_error.UnknownHost("Unknown Host: %s" % masterId)
                # grab slaves list first to ensure operation is atomic
                slaves = self.jobMasters[data['masterId']]['slaves']
                self.setSlaveLimit(masterId, 0)
                for slaveId in slaves:
                    self.stopSlave(slaveId, data['delayed'])
            elif data['action'] == 'stopSlave':
                self.stopSlave(data['slaveId'], data['delayed'])
            elif data['action'] == 'stopJob':
                self.stopJob(data['jobId'])
            elif data['action'] == 'setSlaveLimit':
                self.setSlaveLimit(data['masterId'], data['limit'])
            elif data['action'] == 'setSlaveTTL':
                self.setSlaveTTL(data['slaveId'], data['TTL'])
            elif data['action'] == 'clearCache':
                self.clearCache(data['masterId'])
            else:
                raise mcp_error.IllegalCommand('Unknown action: %s' % \
                                                   data['action'])
        else:
            raise mcp_error.ProtocolError('Unknown Protocol Version: %d' % \
                                              data['protocolVersion'])

    def checkIncomingCommands(self):
        dataStr = self.commandQueue.read()
        while dataStr:
            try:
                data = simplejson.loads(dataStr)
            except Exception, e:
                print >> self.log, "JSON Error decoding command %s: %s\n%s" % \
                    (e.__class__.__name__, str(e), dataStr)
                self.log.flush()
            else:
                self.handleCommand(data)
            dataStr = self.commandQueue.read()

    def checkJobLoad(self):
        # generator is to prevent None from being split, which can happen
        # if we stumble on a new job slave
        for job in [x for x in self.jobCounts if x]:
            version, suffix = job.split(':')
            jobCount = self.jobCounts.get(job, 0)
            slaveCount = self.jobSlaveCounts.get(job, 0)
            if jobCount > slaveCount:
                self.demandJobSlave(version, suffix)

    def respawnJob(self, slaveId):
        jobId = self.jobSlaves.get(slaveId, {}).get('jobId')
        job = self.jobs.get(jobId, {})
        jobData = job.get('data', None)
        if jobData and (job.get('status', ('', ''))[0] not in ('finished',
                                                               'failed')):
            print >> self.log, "Respawning Job:", jobId
            self.log.flush()
            self.handleJob(jobData, force = True)

    def slaveOffline(self, slaveId):
        # only respawn job after slave is offline to avoid race conditions
        self.respawnJob(slaveId)
        masterId = slaveId.split(':')[0]
        if slaveId in self.jobSlaves:
            del self.jobSlaves[slaveId]
        if slaveId in self.getMaster(masterId)['slaves']:
            self.jobMasters[masterId]['slaves'].remove(slaveId)

    def requestMasterStatus(self, masterId = 'masters'):
        control = {'protocolVersion' : PROTOCOL_VERSION,
                   'action' : 'status',
                   'node' : masterId}
        self.controlTopic.send(simplejson.dumps(control))

    def requestSlaveStatus(self, slaveId = 'slaves'):
        control = {'protocolVersion' : PROTOCOL_VERSION,
                   'action' : 'status',
                   'node' : slaveId}
        self.controlTopic.send(simplejson.dumps(control))

    # warning: deliberate side effect of instantiating a master if
    # one did not exist
    def getMaster(self, masterId):
        if masterId not in self.jobMasters:
            self.requestMasterStatus(masterId)
        return self.jobMasters.setdefault(masterId, {'slaves' : [],
                                                     'arch' : None,
                                                     'limit' : None})

    # warning: deliberate side effect of instantiating a slave (and master) if
    # one did not exist
    def getSlave(self, slaveId):
        masterId = slaveId.split(':')[0]
        master = self.getMaster(masterId)
        if slaveId not in master['slaves']:
            master['slaves'].append(slaveId)
        if slaveId not in self.jobSlaves:
            self.requestSlaveStatus(slaveId)
        return self.jobSlaves.setdefault(slaveId, {'status': None,
                                                   'jobId' : None,
                                                   'type' : None})

    @logErrors
    def handleResponse(self, data):
        if data['protocolVersion'] == 1:
            node = data['node']
            event = data['event']
            if event == 'masterOffline':
                if node in self.jobMasters:
                    for slaveId in self.jobMasters[node]['slaves'][:]:
                        self.slaveOffline(slaveId)
                del self.jobMasters[node]
            elif event == 'masterStatus':
                oldInfo = self.getMaster(node)
                oldSlaves = oldInfo.get('slaves', [])
                for slaveId in [x for x in oldSlaves \
                                    if x not in data['slaves']]:
                    self.slaveOffline(slaveId)
                for key in 'arch', 'slaves', 'limit':
                    if key in data:
                        oldInfo[key] = data[key]
            elif event == 'slaveStatus':
                # a slave status can come from a master or a slave. simply
                # splitting on : will always yield the master's name
                master = self.getMaster(node.split(':')[0])
                slaveId = data['slaveId']
                if data['status'] != 'offline':
                    self.jobSlaves[slaveId] = \
                        {'status' : (data['status'] == 'running') \
                             and 'idle' or data['status'],
                         'jobId': None,
                         'type': data['type']}
                    if slaveId not in master['slaves']:
                        master['slaves'].append(slaveId)
                    if data['status'] == 'building':
                        # remove it from the demand queue
                        count = self.demandCounts.get(data['type'], 0)
                        count = max(count - 1, 0)
                        self.demandCounts[data['type']] = count
                        # add it to job slaves immediately to prevent race
                        # conditions
                        count = self.jobSlaveCounts.get(data['type'], 0)
                        self.jobSlaveCounts[data['type']] = count + 1
                elif data['status'] == 'offline':
                    self.slaveOffline(slaveId)
                    count = self.jobSlaveCounts.get(data['type'], 0)
                    count = max(count - 1, 0)
                    self.jobSlaveCounts[data['type']] = count
            elif event == 'jobStatus':
                slave = self.getSlave(node)
                jobId = data['jobId']
                firstSeen = jobId not in self.jobs
                job = self.jobs.setdefault(jobId, \
                    {'status' : (data['status'],
                                 data['statusMessage']),
                     'slaveId' : node,
                     'data' : 'jobData' in data and data['jobData'] or None})
                if data['status'] == 'running':
                    if not firstSeen:
                        # only tweak the job count if we knew of a job. useful
                        # for graceful recovery.
                        count = self.jobCounts.get(slave['type'], 0)
                        count = max(count - 1, 0)
                        self.jobCounts[slave['type']] = count
                    self.jobSlaves[node]['jobId'] = jobId
                    self.jobs[jobId]['slaveId'] = node
                    self.jobSlaves[node]['status'] = 'active'
                elif data['status'] in ('finished', 'failed'):
                    self.jobs[jobId]['slaveId'] = None
                    self.jobSlaves[node]['jobId'] = None
                    self.jobSlaves[node]['status'] = 'idle'
                    if jobId in self.logFiles:
                        del self.logFiles[jobId]
                self.jobs[jobId]['status'] = (data['status'],
                                              data['statusMessage'])
            elif event == 'postJobOutput':
                self.postQueue.send(data['dest'],
                                    simplejson.dumps({'uuid': data['jobId'],
                                                      'urls': data['urls']}))
            elif event == 'jobLog':
                slave = self.getSlave(node)
                self.logJob(data['jobId'], data['message'])
        else:
            raise mcp_error.ProtocolError(\
                "Unknown Protocol Version: %d\ndata: %s" % \
                    (data['protocolVersion'], str(data)))

    def checkResponses(self):
        dataStr = self.responseTopic.read()
        while dataStr:
            data = simplejson.loads(dataStr)
            self.handleResponse(data)
            dataStr = self.responseTopic.read()

    def run(self):
        self.running = True
        self.requestMasterStatus()
        self.requestSlaveStatus()
        try:
            while self.running:
                self.checkIncomingCommands()
                self.checkResponses()
                self.checkJobLoad()
                time.sleep(0.1)
        finally:
            self.disconnect()

    def disconnect(self):
        self.running = False
        self.commandQueue.disconnect()
        self.responseTopic.disconnect()
        self.controlTopic.disconnect()
        for name in self.demand:
                self.demand[name].disconnect()
        for name in self.jobQueues:
            self.jobQueues[name].disconnect()
        self.postQueue.disconnect()


def main():
    cfg = config.McpConfig()
    cfg.read(os.path.join(os.path.sep, 'srv', 'rbuilder', 'mcp', 'config'))
    mcpServer = MCPServer(cfg)
    mcpServer.run()

def runDaemon():
    pidFile = os.path.join(os.path.sep, 'var', 'run', 'mcp.pid')
    if os.path.exists(pidFile):
        f = open(pidFile)
        pid = f.read()
        f.close()
        statPath = os.path.join(os.path.sep, 'proc', pid, 'stat')
        if os.path.exists(statPath):
            f = open(statPath)
            name = f.read().split()[1][1:-1]
            if name == 'mcp':
                print >> sys.stderr, "MCP already running as: %s" % pid
                sys.stderr.flush()
                sys.exit(-1)
            else:
                # pidfile doesn't point to an mcp
                os.unlink(pidFile)
        else:
            # pidfile is stale
            os.unlink(pidFile)
    pid = os.fork()
    if not pid:
        os.setsid()
        devNull = os.open(os.devnull, os.O_RDWR)
        os.dup2(devNull, sys.stdout.fileno())
        os.dup2(devNull, sys.stderr.fileno())
        os.dup2(devNull, sys.stdin.fileno())
        os.close(devNull)
        pid = os.fork()
        if not pid:
            f = open(pidFile, 'w')
            f.write(str(os.getpid()))
            f.close()
            main()
            os.unlink(pidFile)
