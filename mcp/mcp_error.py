#
# Copyright (c) 2005-2006 rPath, Inc.
#
# All rights reserved
#

class MCPError(Exception):
    pass

class ProtocolError(MCPError):
    def __init__(self, msg = "Protocol Error"):
        self.msg = msg
    def __str__(self):
        return self.msg

class UnknownJobType(MCPError):
    def __init__(self, msg = "Unknown job type. Job will not be run."):
        self.msg = msg
    def __str__(self):
        return self.msg

class UnknownJob(MCPError):
    def __init__(self, msg = "Unknown job ID. Job will not be run."):
        self.msg = msg
    def __str__(self):
        return self.msg

class IllegalCommand(MCPError):
    def __init__(self, msg = "Illegal Command"):
        self.msg = msg
    def __str__(self):
        return self.msg

class UnknownHost(MCPError):
    def __init__(self, msg = "Unknown Host"):
        self.msg = msg
    def __str__(self):
        return self.msg

class JobConflict(MCPError):
    def __init__(self, msg = "Job already in progress"):
        self.msg = msg
    def __str__(self):
        return self.msg

class InternalServerError(MCPError):
    def __str__(self):
        return "An internal server error has occured"
