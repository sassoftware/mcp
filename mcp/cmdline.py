#!/usr/bin/python
#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import optparse
import os
import simplejson
import sys
import time

from conary import conarycfg
from conary import conaryclient
from conary.conaryclient import cmdline
from mint import buildtypes

from mcp import client
from mcp import queue

def submitJob(buildData):
    mcpClient = client.MCPClient(cfg)


def main():
    usage = "usage: %prog [options] trovespec"
    parser = optparse.OptionParser(usage)

    parser.add_option("-d", "--dir", dest = "directory", default = '.',
                      help = "write output to DIR", metavar = "DIR")

    parser.add_option("-q", "--quiet",
                      action = "store_false", dest = "verbose", default = True,
                      help = "don't print status messages to stdout")

    parser.add_option("-T", "--title", dest = "title", help = "Project Title")

    parser.add_option("-n", "--name", dest = "name", help = "Name of build")

    parser.add_option("-l", "--label", dest = 'label',
                     help = "Label of project")

    parser.add_option("-c", "--config", dest = 'config',
                      help = "conary config", action = 'append', default = [])

    parser.add_option("-H", "--host", dest = 'queueHost',
                      help = "Hostname of message queue", default = '127.0.0.1')

    parser.add_option("-P", "--port", dest = 'queuePort', type="int",
                      help = "Port of message queue", default = 61613)

    parser.add_option("-t", "--type", dest = "TYPE",
                     help = "Type of build")

    parser.add_option("-a", "--advanced", dest = "advanced",
                      help = "Build specific avanced options",
                      default = [], action = "append")

    (options, args) = parser.parse_args()

    if options.name is None:
        options.name = options.title

    if not len(args) == 1:
        parser.print_help()
        parser.error("trovespec is required")

    missingOptions = [x[0] for x in options.__dict__.iteritems() \
                          if x[1] is None]
    if missingOptions:
        parser.print_help()
        parser.error("These options must be set: %s." % \
                         (', '.join(missingOptions)))

    if options.TYPE not in buildtypes.validBuildTypes:
        parser.print_help()
        parser.error("Type must be one of: %s" % \
                         (', '.join(buildtypes.validBuildTypes)))

    UUID = options.label.split('@')[0] + '-build-' + \
        str(sum([(256 ** x[0]) * x[1] for x in \
                 enumerate([ord(x) for x in os.urandom(8)])]))

    buildData = {}
    buildData['serialVersion'] = 1
    buildData['UUID'] = UUID
    buildData['buildType'] = buildtypes.validBuildTypes[options.TYPE]
    buildData['name'] = options.name

    buildData['project'] = {}
    buildData['project']['hostname'] = options.label.split('.')[0]
    buildData['project']['label'] = options.label
    buildData['project']['conaryCfg'] = '\n'.join(options.config)
    buildData['project']['name'] = options.title

    outputQueue = ''.join([hex(ord(x))[2:] for x in os.urandom(16)])

    buildData['outputQueue'] = outputQueue
    buildData['type'] = 'build'

    cfg = client.MCPClientConfig()
    cfg.configLine('queueHost %s' % options.queueHost)
    cfg.configLine('queuePort %d' % options.queuePort)
    mcpClient = client.MCPClient(cfg)

    buildData['data'] = {}
    buildData['data']['jsversion'] = str(mcpClient.getJSVersion())
    buildData['data'].update(dict([x.split(' ', 1) for x in options.advanced]))

    cfg = conarycfg.ConaryConfiguration(True)
    cfg.initializeFlavors()

    cc = conaryclient.ConaryClient(cfg)
    nc = cc.getRepos()

    n, v, f = cmdline.parseTroveSpec(args[0])
    NVF = nc.findTrove(None, (n, v, f), cc.cfg.flavor)[0]

    buildData['troveName'] = NVF[0]
    buildData['troveVersion'] = NVF[1].freeze()
    buildData['troveFlavor'] = NVF[2].freeze()

    mcpClient.submitJob(simplejson.dumps(buildData))

    outputQueue = queue.Queue(options.queueHost, options.queuePort, outputQueue, timeOut = None)
    status, statusMessage = None, None
    while status not in ('built', 'finished', 'failed'):
        try:
            newStatus, newStatusMessage = mcpClient.jobStatus(UUID)
        except Exception, e:
            newStatus, newStatusMessage = 'error', e
        if (newStatus != status) or (statusMessage != newStatusMessage):
            status, statusMessage = newStatus, newStatusMessage
            print "%-79s\x0d" % ("%s: %s" % (status, statusMessage)),
            sys.stdout.flush()
        else:
            time.sleep(1)
    print ""

    if status == 'built':
        dataStr = outputQueue.read()
        data = simplejson.loads(dataStr)
        for url, type in data['urls']:
            os.system('wget %s -P %s' % (url, options.directory))
        mcpClient.stopJob(UUID)
    outputQueue.disconnect()

if __name__ == '__main__':
    main()
