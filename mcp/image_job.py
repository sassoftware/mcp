#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

from rmake.lib.apiutils import register, freeze, thaw


class ImageJob(object):
    def __init__(self, mintUrl, jobId, jobData):
        self.mintUrl = mintUrl
        self.jobId = jobId
        self.jobData = jobData

    def __freeze__(self):
        return self.__dict__.copy()

    @classmethod
    def __thaw__(cls, d):
        return cls(**d)
register(ImageJob)
