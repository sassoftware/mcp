#
# Copyright (c) 2006-2007 rPath, Inc.
#
# All rights reserved
#

import os, sys
import pprint
import time
import threading
import simplejson

from conary import conaryclient
from conary import conarycfg
from conary.errors import TroveNotFound
from conary.deps import deps
import logging
log = logging

from mcp import queue
from mcp import mcp_error
from mcp import config
from mcp import jobstatus
from mcp import slavestatus
import traceback

PROTOCOL_VERSION = 1

dumpEvery = 10

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
                log.error(e.__class__.__name__ + ": " + str(e))
                log.error('\n'.join(traceback.format_tb(bt)))
                e = mcp_error.InternalServerError()
            res = True, (e.__class__.__name__, str(e))
        if type(command) != dict:
            log.error("command is not a dict: %s" % str(command))
        elif 'uuid' not in command:
            log.error("no post address: %s" % str(command))
        else:
            self.postQueue.send(command['uuid'], simplejson.dumps(res))
    return wrapper

def logErrors(func):
    def wrapper(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except:
            exc, e, bt = sys.exc_info()
            log.error("%s %s" % ("Response Exception: (" + \
                e.__class__.__name__ + ')', str(e)))
            log.error('\n'.join(traceback.format_tb(bt)))
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
            logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename= os.path.join(cfg.logPath, 'mcp.log'),
                    filemode='a')
        # command queue is for sending commands *to* the mcp
        self.commandQueue = queue.Queue(cfg.queueHost, cfg.queuePort, 'command',
                                        namespace = cfg.namespace, timeOut = 0)
        # control topic is for mcp to send commands to all other units
        self.controlTopic = queue.Topic(cfg.queueHost, cfg.queuePort, 'control',
                                        namespace = cfg.namespace,
                                        autoSubscribe = False)

        # response Topic will be used by other nodes talking to mcp
        self.responseTopic = queue.Topic(cfg.queueHost,
                                         cfg.queuePort, 'response',
                                         namespace = cfg.namespace,
                                         timeOut = 0)

        self.postQueue = queue.MultiplexedQueue(cfg.queueHost, cfg.queuePort,
                                                autoSubscribe = False,
                                                timeOut = 0)

        # job specific control topic will be a separate avenue to send control
        # messages to whichever jobslave is currently serving a given job.
        # message format will be identical to controlTopic messages
        self.jobControlQueue = queue.MultiplexedQueue( \
            cfg.queueHost, cfg.queuePort, namespace = cfg.namespace,
            autoSubscribe = False)

    def logJob(self, jobId, message):
        message = '\n'.join([x for x in message.splitlines() if x])
        if self.cfg.logPath:
            if jobId not in self.logFiles:
                self.logFiles[jobId] = \
                    open(os.path.join( \
                        self.cfg.logPath,
                        jobId + time.strftime('-%Y-%m-%d_%H:%M:%S')), 'w')
            logFile = self.logFiles[jobId]
            logFile.write(message)
        else:
            print jobId + ':', message

    def getVersion(self, version):
        cfg = conarycfg.ConaryConfiguration(True)
        cc = conaryclient.ConaryClient(cfg)
        try:
            l = version and (self.cfg.slaveTroveLabel + '/' + version) \
                or self.cfg.slaveTroveLabel
            troves = cc.repos.findTrove( \
                None, (self.cfg.slaveTroveName, l, None))
        except TroveNotFound:
            return []
        return sorted(troves)[-1]

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
            data['troveSpec'] = '%s=%s/%s[is: %s]' % (self.cfg.slaveTroveName,
                                              self.cfg.slaveTroveLabel, version,
                                                      suffix)
            log.debug("demanding slave: %s on %s" % (data['troveSpec'], demandName))
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
        UUID = simplejson.loads(data)['UUID']
        log.info('Placing %s on %s' % (UUID, queueName))
        self.jobQueues[queueName].send(data)
        count = self.jobCounts.get(type, 0) + 1
        self.jobCounts[type] = count

    def handleJob(self, dataStr, force = False):
        try:
            data = simplejson.loads(dataStr)
        except Exception, e:
            raise mcp_error.ProtocolError('unable to parse job: %s' % str(e))
        UUID = data['UUID']
        if (not force) and (UUID in self.jobs) and \
                (self.jobs[UUID]['status'][0] not in (jobstatus.FINISHED,
                                                      jobstatus.FAILED)):
            raise mcp_error.JobConflict
        type = data['type']
        version = ''
        if type == 'build':
            version = \
                self.getTrailingRevision(data['data']['jsversion'])
            suffix = getSuffix(data['troveFlavor'])
        elif type == 'cook':
            version = self.getTrailingRevision(version = '')
            suffix = getSuffix(data['data']['arch'])
        else:
            raise mcp_error.UnknownJobType('Unknown job type: %s' % type)
        self.jobs[data['UUID']] = \
            {'status' : (jobstatus.WAITING, 'Waiting to be processed'),
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

    def stopJob(self, jobId, useQueue = True):
        if jobId not in self.jobs:
            raise mcp_error.UnknownJob('Unknown Job ID: %s' % jobId)
        slaveId = self.jobs[jobId]['slaveId']
        control = {'protocolVersion' : PROTOCOL_VERSION,
                   'node' : 'slaves',
                   'action' : 'stopJob',
                   'jobId' : jobId}
        if slaveId:
            # the slave only has one job running at a time, but the jobId
            # is included in the control packet to ensure a race condition can't
            # kill the wrong job.
            if useQueue:
                # send kill message to job specific control queue, due to the
                # fact that the job may not be active when the stop command is
                # sent.
                self.jobControlQueue.send(slaveId, simplejson.dumps(control))
                return
        # fallback. used if no slave is known, or if we weren't supposed to
        # use a queue.
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
            if data['action'] != 'jobStatus':
                log.info("Incoming command '%s'" % data['action'])
            else:
                log.debug("Incoming command '%s'" % data['action'])
            log.debug("Payload: %s" % repr(data))
            if data['action'] == 'submitJob':
                return self.handleJob(data['data'])
            elif data['action'] == 'getJSVersion':
                return self.getTrailingRevision(version = '').split('-')[0]
            elif data['action'] == 'nodeStatus':
                # this casting protects the jobMasters data structure, since we
                # will be modifying it. if the data must be changed radically,
                # it might be worth exploring copy.deepcopy vs refactoring this
                res = dict([(x[0], dict(x[1].iteritems())) \
                                for x in self.jobMasters.iteritems()])
                for data in res.values():
                    data['slaves'] = dict([(x, self.jobSlaves.get(x)) \
                                               for x in data['slaves']])
                return res
            elif data['action'] == 'jobStatus':
                jobId = data.get('jobId')
                if jobId and jobId not in self.jobs:
                    raise mcp_error.UnknownJob('Unknown job Id: %s' % jobId)
                if jobId:
                    r = self.jobs[jobId]['status']
                else:
                    # scrub the "data" element it's large and not related to
                    # status
                    r = dict([(x[0], dict([y for y in x[1].iteritems() \
                                                     if y[0] != 'data'])) \
                                    for x in self.jobs.iteritems()])
                return r
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
            elif data['action'] == 'ackJob':
                self.stopJob(data['jobId'], useQueue = False)
            elif data['action'] == 'setSlaveLimit':
                self.setSlaveLimit(data['masterId'], data['limit'])
            elif data['action'] == 'setSlaveTTL':
                self.setSlaveTTL(data.get('slaveId'), data['TTL'])
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
                log.error("JSON Error decoding command %s: %s\n%s" % (e.__class__.__name__, str(e), dataStr))
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
        if jobData and (job.get('status', ('', ''))[0] not in \
                            (jobstatus.FINISHED, jobstatus.FAILED)):
            log.info("Respawning Job: %s" % jobId)
            self.handleJob(jobData, force = True)

    def slaveOffline(self, slaveId):
        # only respawn job after slave is offline to avoid race conditions
        self.respawnJob(slaveId)
        masterId = slaveId.split(':')[0]
        if slaveId in self.jobSlaves:
            type = self.jobSlaves[slaveId]['type']
            count = self.jobSlaveCounts.get(type, 0)
            count = max(count - 1, 0)
            log.info("Setting slave count of type: %s to %d" % (type, count))
            self.jobSlaveCounts[type] = count
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
            log.info("Handling response event '%s' from node '%s'" % (event, node))
            log.debug("Payload: %s" % repr(data))
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
                # ensure we record each slave. useful when we just started up
                # and don't know of the slave yet.
                for slaveId in data['slaves']:
                    self.getSlave(slaveId)
                for key in 'arch', 'slaves', 'limit':
                    if key in data:
                        oldInfo[key] = data[key]
            elif event == 'slaveStatus':
                # a slave status can come from a master or a slave. simply
                # splitting on : will always yield the master's name
                master = self.getMaster(node.split(':')[0])
                slaveId = data['slaveId']
                if data['status'] != slavestatus.OFFLINE:
                    self.jobSlaves[slaveId] = \
                        {'status' : data['status'],
                         'jobId': None,
                         'type': data['type']}
                    if slaveId not in master['slaves']:
                        master['slaves'].append(slaveId)
                    if data['status'] == slavestatus.BUILDING:
                        # remove it from the demand queue
                        count = self.demandCounts.get(data['type'], 0)
                        count = max(count - 1, 0)
                        self.demandCounts[data['type']] = count
                        # add it to job slaves immediately to prevent race
                        # conditions
                        count = self.jobSlaveCounts.get(data['type'], 0)
                        self.jobSlaveCounts[data['type']] = count + 1
                else:
                    self.slaveOffline(slaveId)
            elif event == 'jobStatus':
                slave = self.getSlave(node)
                jobId = data['jobId']
                firstSeen = jobId not in self.jobs
                job = self.jobs.setdefault(jobId, \
                    {'status' : (data['status'],
                                 data['statusMessage']),
                     'slaveId' : node,
                     'data' : 'jobData' in data and data['jobData'] or None})
                if data['status'] == jobstatus.RUNNING:
                    if not firstSeen:
                        # only tweak the job count if we knew of a job. useful
                        # for graceful recovery.
                        count = self.jobCounts.get(slave['type'], 0)
                        count = max(count - 1, 0)
                        self.jobCounts[slave['type']] = count
                    self.jobSlaves[node]['jobId'] = jobId
                    self.jobs[jobId]['slaveId'] = node
                    self.jobSlaves[node]['status'] = slavestatus.ACTIVE
                elif data['status'] in (jobstatus.FINISHED, jobstatus.FAILED):
                    self.jobs[jobId]['slaveId'] = None
                    self.jobSlaves[node]['jobId'] = None
                    self.jobSlaves[node]['status'] = slavestatus.IDLE
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
            lastDump = time.time()
            while self.running:
                self.checkIncomingCommands()
                self.checkResponses()
                self.checkJobLoad()
                time.sleep(0.1)
                if time.time() > (lastDump + dumpEvery):
                    self.dump()
                    lastDump = time.time()
        finally:
            self.disconnect()

    def dump(self):
        log.debug("demandCounts: %s" % pprint.pformat(self.demandCounts))
        log.debug("jobCounts: %s" % pprint.pformat(self.jobCounts))
        log.debug("jobQueues: %s" % pprint.pformat(self.jobQueues))
        log.debug("demand: %s" % pprint.pformat(self.demand))
        log.debug("jobSlaveCounts: %s" % pprint.pformat(self.jobSlaveCounts))
        log.debug("jobSlaves: %s" % pprint.pformat(self.jobSlaves))
        log.debug("jobs: %s" % pprint.pformat(self.jobs))
        log.debug("jobMasters: %s" % pprint.pformat(self.jobMasters))

    def disconnect(self):
        self.running = False
        self.commandQueue.disconnect()
        self.responseTopic.disconnect()
        self.controlTopic.disconnect()
        self.jobControlQueue.disconnect()
        for name in self.demand:
                self.demand[name].disconnect()
        for name in self.jobQueues:
            self.jobQueues[name].disconnect()
        self.postQueue.disconnect()


def main():
    cfg = config.MCPConfig()
    cfg.read(os.path.join(os.path.sep, 'srv', 'rbuilder', 'mcp', 'config'))
    mcpServer = MCPServer(cfg)
    log.info("MCP server starting")
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
