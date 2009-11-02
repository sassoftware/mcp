#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved
#

import logging


def setupLogging(logLevel=logging.INFO, toStderr=True, toFile=None):
    """
    Set up a root logger with default options and possibly a file to
    log to.
    """
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s')

    if isinstance(logLevel, basestring):
        logLevel = logging.getLevelName(logLevel.upper())

    rootLogger = logging.getLogger()
    rootLogger.setLevel(logLevel)
    rootLogger.handlers = []

    if toStderr:
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        rootLogger.addHandler(streamHandler)

    if toFile:
        fileHandler = logging.FileHandler(toFile)
        fileHandler.setFormatter(formatter)
        rootLogger.addHandler(fileHandler)
