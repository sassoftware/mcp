#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import time
import weakref
from rmake.lib import apirpc
from rmake.messagebus import busclient
from mcp.messagebus import messages


class BusNode(apirpc.ApiServer):
    nodeType = None
    sessionClass = 'Anonymous'
    subscriptions = []
    timerPeriod = 5

    def __init__(self, (busHost, busPort), nodeInfo=None, logger=None):
        apirpc.ApiServer.__init__(self, logger=logger)

        self.subscriptions = list(self.subscriptions)
        if nodeInfo:
            self.nodeInfo = nodeInfo
        elif self.nodeType:
            self.nodeInfo = self.nodeType()
        else:
            self.nodeInfo = None

        self.bus = busclient.MessageBusClient(busHost, busPort,
                dispatcher=self, sessionClass=self.sessionClass,
                subscriptions=self.subscriptions)

        self.lastTimer = time.time()
        self.onStart()

    def onStart(self):
        pass

    def onTimer(self):
        pass

    def handleRequestIfReady(self, sleepTime):
        self.bus.poll(sleepTime, maxIterations=1)
        if time.time() - self.lastTimer >= self.timerPeriod:
            self.onTimer()
            self.lastTimer = time.time()

    def messageReceived(self, message):
        name = message.__class__.__name__
        if isinstance(message, messages.ImageCommandMessage):
            method = 'do' + name
        else:
            method = 'handle' + name
        if hasattr(self, method):
            getattr(self, method)(message)

    def handleConnectedResponse(self, message):
        if self.nodeInfo:
            m = messages.RegisterNodeMessage()
            m.set(self.nodeInfo)
            self.bus.sendMessage('/register', m)
