#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import logging


class MessageBusLogger(logging.Logger):
    @classmethod
    def new(cls, name=None):
        ret = logging.getLogger(name)
        ret.__class__ = cls
        return ret

    def close(self):
        pass

    # Message bus logger interface
    def logMessage(self, message, fromSession=None):
        pass

    def setSessionId(self, sessionId):
        self.debug("Connected %s", sessionId)
