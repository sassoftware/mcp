#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import os
from rmake.lib.apiutils import register, freeze, thaw


class ImageJob(object):
    def __init__(self, mint_url, job_id, job_data, uuid=None):
        self.mint_url = mint_url
        self.job_id = job_id
        self.job_data = job_data
        self.uuid = uuid

        self.node_id = None

    def __freeze__(self):
        return dict(mint_url=self.mint_url, job_id=self.job_id,
                job_data=self.job_data, uuid=self.uuid)

    @classmethod
    def __thaw__(cls, d):
        return cls(**d)

    # Scheduler helpers
    def assign_uuid(self):
        self.uuid = os.urandom(16).encode('hex')
register(ImageJob)
