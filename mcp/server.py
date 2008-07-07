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
import socket

import epdb

from conary import conaryclient
from conary import conarycfg
from conary.repository.errors import InsufficientPermission
from conary.errors import TroveNotFound
from conary.deps import deps
from conary.lib import util
import logging
log = logging

from mcp import queue
from mcp import mcp_error
from mcp import mcp_log
from mcp import config
from mcp import jobstatus
from mcp import slavestatus
import traceback

PROTOCOL_VERSION = 1

dumpEvery = 10

SLAVE_SET_NAME = 'group-jobslave-set'
DEBUG_PATH = os.path.join(os.path.sep, 'srv', 'rbuilder', 'mcp', 'debug')

def logTraceback(logger, msg = "Traceback:"):
    exc, e, bt = sys.exc_info()
    logger(msg)
    logger(e.__class__.__name__ + ": " + str(e))
    logger('\n'.join(traceback.format_tb(bt)))

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
                logTraceback(log.error)
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
            logTraceback(log.error, "Response Exception:")
    return wrapper

def decodeJson(dataStr):
    try:
        data = simplejson.loads(dataStr)
    except:
        logTraceback(log.error, "JSON error decoding command:")
        log.error('Command was: %s' % dataStr)
        return False, None
    else:
        return True, data

class MCPServer(object):
    def __init__(self, cfg):
        self.waitingJobs = []
        self.jobQueues = {}
        self.jobSlaves = {}
        self.jobs = {}
        self.jobMasters = {}
        self.logFiles = {}

        self.cfg = cfg
        if cfg.logPath:
            mcp_log.addRootLogger(level=cfg.logLevel,
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

    def saveJobs(self):
        fn = os.path.join(self.cfg.basePath, 'jobs')
        f = open(fn, 'w')
        try:
            for conn in [x.connectionName for x in self.jobQueues.values()]:
                queueName = conn.split('/')[-1]
                jobQueue = queue.Queue(self.cfg.queueHost,
                        self.cfg.queuePort, queueName,
                        namespace = self.cfg.namespace, timeOut = 0,
                        autoSubscribe = True)
                dataStr = jobQueue.read()
                while dataStr:
                    f.write(dataStr)
                    f.write('\n')
                    dataStr = jobQueue.read()
                jobQueue.disconnect()
        finally:
            f.close()

    def loadJobs(self):
        fn = os.path.join(self.cfg.basePath, 'jobs')
        if os.path.exists(fn):
            try:
                f = open(fn)
                for dataStr in f.readlines():
                    self.handleJob(dataStr.strip())
            finally:
                # no matter what, only read this data once.
                os.unlink(fn)

    def logJob(self, jobId, message):
        message = '\n'.join([x for x in message.splitlines() if x])
        if self.cfg.logPath:
            if jobId not in self.logFiles:
                try:
                    logFileName = os.path.join(self.cfg.logPath, 'jobs',
                            jobId + time.strftime('-%Y-%m-%d_%H:%M:%S'))
                    self.logFiles[jobId] = open(logFileName, 'w')
                except:
                    log.error("Could not open logfile for %s" % (jobId))
                else:
                    log.info("Logging job %s to %s" % \
                            (jobId, self.logFiles[jobId].name))
            logFile = self.logFiles.get(jobId)
            if logFile:
                logFile.write(message + '\n')
                logFile.flush()
        else:
            print jobId + ':', message

    def getTopGroupLabel(self, cc):
        '''Get the label on which the appliance's top-level group resides.'''
        if self.cfg.slaveSetLabel:
            return self.cfg.slaveSetLabel
        try:
            # Try getting the top-level group where distro-release detects it
            group = open('/etc/sysconfig/appliance-group').read().strip()
            assert group
        except:
            # The mcp itself is probably the best bet if
            # a top-level group was not found on startup
            group = 'mcp'

        n, v, f = cc.db.findTrove(None, (group, None, None))[0]
        return v.trailingLabel().asString()

    def getSlaveList(self):
        '''
        Obtain a list of jobslaves and return a tuple of a dictionary
        mapping revision to (name, version), and the latest slave
        revision.
        '''

        cfg = conarycfg.ConaryConfiguration(True)
        cc = conaryclient.ConaryClient(cfg)
        nc = cc.getRepos()
        search = cc.getSearchSource(flavor=0)

        # Get latest jobslave set matching 'version'
        query_version = self.getTopGroupLabel(cc)
        if self.cfg.slaveSetVersion:
            query_version += '/' + self.cfg.slaveSetVersion
        else:
            log.warning('Running without an explicit jobslave set version. '
                'Using latest on label.')
        troveSpec = (SLAVE_SET_NAME, query_version, None)
        log.debug('Jobslave set query: %s=%s[%s]' % troveSpec)

        # Try to locate the specified jobslave set
        try:
            results = search.findTroves([troveSpec],
                bestFlavor=False)[troveSpec]
        except TroveNotFound:
            log.error('Trove not found while stocking jobslave set. '
                'Query: %s=%s', SLAVE_SET_NAME, query_version)
            raise mcp_error.SlaveNotFoundError("The appliance could not "
                "locate an appropriate jobslave set.")
        except InsufficientPermission:
            log.error('Not entitled to jobslave set. Query: %s=%s',
                SLAVE_SET_NAME, query_version)
            raise mcp_error.NotEntitledError()

        latest = max(x[1] for x in results)
        troveSpec = sorted(x for x in results if x[1] == latest)[0]
        log.debug('Using jobslave set %s=%s[%s]' % troveSpec)

        # Create a mapping of revision -> (name, version) from the
        # contents of the jobslave set
        slaveSetTrove = nc.getTrove(troveSpec[0], troveSpec[1],
            troveSpec[2], withFiles=False)
        slaveDict = {}
        for name, version, flavor in slaveSetTrove.iterTroveList(
          strongRefs=True):
            log.debug('Adding jobslave %s=%s[%s]' % (name, version, flavor))
            revision = version.trailingRevision().getVersion()

            if revision in slaveDict and version < slaveDict[revision][1]:
                # Don't replace if it's older
                continue

            slaveDict[revision] = (name, version)

        if not slaveDict:
            # Empty set?
            log.error('Got empty jobslave set! %s=%s[%s]' % troveSpec)
            raise mcp_error.SlaveNotFoundError("The appliance could not "
                "locate any jobslaves.")

        # Get the latest version out of all the slave tups
        maxVersion = max(x[1] for x in slaveSetTrove.iterTroveList(
            strongRefs=True))
        maxRevision = maxVersion.trailingRevision().getVersion()

        return slaveDict, maxRevision

    def getVersion(self, version=None):
        slaveDict, maxRevision = self.getSlaveList()

        if version:
            if version in slaveDict:
                slave = slaveDict[version]
            else:
                # If we don't know how to build this version, just use
                # the latest slave in the set and log a warning. This
                # will always happen for builds originally created on
                # 3.x and restarted on 4.x.
                log.warning('Could not find jobslave (version %r), falling '
                    'back to latest (version %r)', version, maxRevision)
                slave = slaveDict[maxRevision]
        else:
            slave = slaveDict[maxRevision]

        return slave

    def addJobQueue(self, suffix):
        jobName = 'job:%s' % suffix
        self.jobQueues[jobName] = queue.Queue(self.cfg.queueHost,
                                         self.cfg.queuePort, jobName,
                                         namespace = self.cfg.namespace,
                                         autoSubscribe = False)

    def addJob(self, version, suffix, dataStr):
        queueName = 'job:%s' % suffix
        if queueName not in self.jobQueues:
            self.addJobQueue(suffix)

        valid, data = decodeJson(dataStr)
        if not valid:
            log.warning("Job could not be added. Invalid data found: '%s'" % \
                    dataStr)
            return None

        UUID = data['UUID']
        log.info('Placing %s on %s' % (UUID, queueName))
        data['jobSlaveNVF'] = '%s=%s[is: %s]' % (version[0],
                                          str(version[1]), suffix)
        dataStr = simplejson.dumps(data)
        self.jobQueues[queueName].send(dataStr)
        self.waitingJobs.append(UUID)

    def handleJob(self, dataStr):
        valid, data = decodeJson(dataStr)
        if not valid:
            return None

        UUID = data['UUID']
        if (UUID in self.jobs):
            raise mcp_error.JobConflict
        type = data['type']
        version = ''
        if type == 'build':
            slave = self.getVersion(data['data']['jsversion'])
            suffix = getSuffix(data['troveFlavor'])
        elif type == 'cook':
            slave = self.getVersion()
            suffix = getSuffix(data['data']['arch'])
        else:
            raise mcp_error.UnknownJobType('Unknown job type: %s' % type)
        if slave:
            self.jobs[data['UUID']] = \
                {'status' : (jobstatus.WAITING, 'Waiting to be processed'),
                'slaveId' : None,
                'data' : dataStr}
            self.addJob(slave, suffix, dataStr)
        else:
            self.jobs[data['UUID']] = \
                    {'status' : (jobstatus.FAILED,
                        "Unable to find suitable jobslave"),
                    'slaveId' : None,
                    'data' : dataStr}
        return data['UUID']

    def stopSlave(self, slaveId):
        if slaveId not in self.jobSlaves:
            raise mcp_error.UnknownHost("Unknown Host: %s" % slaveId)

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
        if not slaveId:
            self.jobs[jobId]['status'] = (jobstatus.KILLED,
                    "Job killed at user's request")

        control = {'protocolVersion' : PROTOCOL_VERSION,
                   'node' : 'slaves',
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
            if data['action'] != 'jobStatus':
                log.info("Incoming command '%s'" % data['action'])
            else:
                log.debug("Incoming command '%s'" % data['action'])
            log.debug("Payload: %s" % repr(data))
            if data['action'] == 'submitJob':
                return self.handleJob(data['data'])
            elif data['action'] == 'getJSVersion':
                return self.getVersion()[1].trailingRevision().getVersion()
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
                    killed = self.jobs[jobId]['status'][0] == jobstatus.KILLED
                    if not killed and (jobId in self.waitingJobs):
                        ind = self.waitingJobs.index(jobId)
                        if ind:
                            statMsg = "Number %d in line for processing" % \
                                    (ind + 1)
                        else:
                            statMsg = "Next in line for processing"
                        r = (jobstatus.WAITING, statMsg)
                    else:
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
                if not data['delayed']:
                    for slaveId in slaves:
                        self.stopSlave(slaveId)
            elif data['action'] == 'stopSlave':
                self.stopSlave(data['slaveId'])
            elif data['action'] == 'stopJob':
                self.stopJob(data['jobId'])
            elif data['action'] == 'setSlaveLimit':
                self.setSlaveLimit(data['masterId'], data['limit'])
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
            valid, data = decodeJson(dataStr)
            if valid:
                self.handleCommand(data)
            dataStr = self.commandQueue.read()

    def isJobKilled(self, jobId):
        job = self.jobs.get(jobId, {})
        return job.get('status', ('', ''))[0] == jobstatus.KILLED

    def handleDeadJobs(self, slaveId):
        jobId = self.jobSlaves.get(slaveId, {}).get('jobId')
        job = self.jobs.get(jobId, {})

        if jobId in self.waitingJobs:
            self.waitingJobs.remove(jobId)

        if self.isJobKilled(jobId):
            job['status'] = (jobstatus.FAILED, "Job killed at user's request")
        elif job.get('status', ('', ''))[0] not in \
                (jobstatus.FAILED, jobstatus.FINISHED):
            job['status'] = (jobstatus.FAILED, "The slave handling this job has died.")

    def closeJobLog(self, jobId):
        if jobId in self.logFiles:
            # compress log file
            # DO NOT use util.execute. there's no way to make this step atomic
            # failure can cause unrecoverably inconsistent state!
            logName = self.logFiles[jobId].name
            del self.logFiles[jobId]
            os.system("/bin/gzip %s" % logName)

    def slaveOffline(self, slaveId):
        # clear the job log when a slave goes down
        jobId = self.jobSlaves.get(slaveId, {}).get('jobId')
        self.closeJobLog(jobId)
        self.handleDeadJobs(slaveId)
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
            log.debug("Handling response event '%s' from node '%s'" % (event, node))
            log.debug("Payload: %s" % repr(data))
            if event == 'masterOffline':
                if node in self.jobMasters:
                    for slaveId in self.jobMasters[node]['slaves'][:]:
                        self.slaveOffline(slaveId)
                    del self.jobMasters[node]
            elif event == 'masterStatus':
                oldInfo = self.getMaster(node)
                oldSlaves = oldInfo.get('slaves', [])
                # remove slaves from the master's list that aren't present
                for slaveId in [x for x in oldSlaves \
                                    if x not in data['slaves']]:
                    self.slaveOffline(slaveId)
                # remove slaves from jobslave list that aren't present
                for slaveId in [x for x in self.jobSlaves if \
                        x.split(':')[0] == node and x not in data['slaves']]:
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
                    jobId = data['jobId']
                    self.jobSlaves[slaveId] = \
                        {'status' : data['status'],
                         'jobId': jobId,
                         'type': data['type']}
                    if slaveId not in master['slaves']:
                        master['slaves'].append(slaveId)
                    if self.isJobKilled(jobId):
                        # slave is associated with a jobId that needs removal
                        self.stopSlave(slaveId)
                else:
                    self.slaveOffline(slaveId)
            elif event == 'jobStatus':
                jobId = data['jobId']
                slave = self.getSlave(node)
                if self.isJobKilled(jobId):
                    # The only exit from the killed state is to the failed
                    # state when the job's jobslave dies
                    slaveId = node
                    slave['jobId'] = jobId
                    self.jobs[jobId]['slaveId'] = slaveId
                    self.stopSlave(slaveId)
                    return
                job = self.jobs.setdefault(jobId, \
                    {'status' : (data['status'],
                                 data['statusMessage']),
                     'slaveId' : node,
                     'data' : 'jobData' in data and data['jobData'] or None})
                if job['status'][0] in (jobstatus.FINISHED, jobstatus.FAILED):
                    log.warning("Status message for job Id: %s ignored. " \
                            "Attempted to change status from %s to %s" % \
                            (jobId, jobstatus.statusNames[job['status'][0]],
                                jobstatus.statusNames[data['status']]))
                    return
                if jobId in self.waitingJobs:
                    self.waitingJobs.remove(jobId)
                if data['status'] == jobstatus.RUNNING:
                    self.jobSlaves[node]['jobId'] = jobId
                    self.jobs[jobId]['slaveId'] = node
                    self.jobSlaves[node]['status'] = slavestatus.ACTIVE
                    if self.jobs[jobId]['status'][0] != data['status']:
                        log.info("Job %s started." % jobId)
                elif data['status'] in (jobstatus.FINISHED, jobstatus.FAILED):
                    self.closeJobLog(jobId)
                    self.jobs[jobId]['slaveId'] = None
                    self.jobSlaves[node]['jobId'] = None
                    self.jobSlaves[node]['status'] = slavestatus.IDLE

                    # log changes of status only once
                    if self.jobs[jobId]['status'][0] != data['status']:
                        if data['status'] == jobstatus.FINISHED:
                            log.info("Job %s finished." % jobId)
                        elif data['status'] == jobstatus.FAILED:
                            log.info("Job %s failed: %s" % (jobId, data['statusMessage']))
                self.jobs[jobId]['status'] = (data['status'],
                                              data['statusMessage'])
            elif event == 'jobLog':
                jobId = data['jobId']
                status = self.jobs.get(jobId,
                        {}).get('status',
                                (None, None))[0]
                if status not in (jobstatus.FINISHED, jobstatus.FAILED):
                    slave = self.getSlave(node)
                    self.logJob(data['jobId'], data['message'])
                else:
                    log.info('%s: message received after job was done: %s' % \
                            (jobId, data['message']))
        else:
            raise mcp_error.ProtocolError(\
                "Unknown Protocol Version: %d\ndata: %s" % \
                    (data['protocolVersion'], str(data)))

    def checkResponses(self):
        dataStr = self.responseTopic.read()
        while dataStr:
            valid, data = decodeJson(dataStr)
            if valid:
                self.handleResponse(data)

            dataStr = self.responseTopic.read()

    def checkDebug(self):
        if os.path.exists(DEBUG_PATH):
            # if any of stdin stderr or stdout are closed, we must use a
            # remote connection
            remote = sys.stdout.closed or sys.stderr.closed or sys.stdin.closed
            # likewise, if they are not tty's we can't interact via them
            remote = remote or \
                    not(sys.stderr.isatty() and \
                    sys.stdout.isatty() and \
                    sys.stdin.isatty())
            if remote:
                try:
                    epdb.serve()
                except socket.error:
                    log.warning('socket error when attempting to start epdb server. Assuming an open connection exists')
                    log.warning('setting a standard breakpoint')
                    epdb.st()
            else:
                epdb.st()

    def run(self):
        self.loadJobs()
        self.running = True
        self.requestMasterStatus()
        self.requestSlaveStatus()
        try:
            try:
                lastDump = time.time()
                while self.running:
                    self.checkIncomingCommands()
                    self.checkResponses()
                    self.checkDebug()
                    time.sleep(0.1)
                    if time.time() > (lastDump + dumpEvery):
                        self.dump()
                        lastDump = time.time()
            except:
                logTraceback(log.error, "Unhandled Exception:")
        finally:
            self.disconnect()

    def dump(self):
        log.debug("jobQueues: %s" % pprint.pformat(self.jobQueues))
        log.debug("jobSlaves: %s" % pprint.pformat(self.jobSlaves))
        log.debug("jobs: %s" % pprint.pformat(self.jobs))
        log.debug("jobMasters: %s" % pprint.pformat(self.jobMasters))
        log.debug("waitingJobs: %s" % pprint.pformat(self.waitingJobs))

    def disconnect(self):
        self.running = False
        self.commandQueue.disconnect()
        self.responseTopic.disconnect()
        self.controlTopic.disconnect()
        self.saveJobs()
        for name in self.jobQueues:
            self.jobQueues[name].disconnect()
        self.postQueue.disconnect()


def main(cfg):
    mcpServer = MCPServer(cfg)
    log.info("MCP server starting")
    try:
        mcpServer.run()
    except: # trap any exception and log it
        logTraceback(log.error, "MCP runtime exception:")
    log.info("MCP server exiting")


def runDaemon():
    cfg = config.MCPConfig()
    cfg.read(os.path.join(os.path.sep, 'srv', 'rbuilder', 'mcp', 'config'))

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
            main(cfg)
            os.unlink(pidFile)
