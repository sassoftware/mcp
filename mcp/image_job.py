#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os
from rmake.lib.apiutils import register, freeze, thaw


class ImageJob(object):
    def __init__(self, rbuilder_url, job_data, uuid=None):
        self.rbuilder_url = rbuilder_url
        self.job_data = job_data
        self.uuid = uuid

        self.node_id = None

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        return self.uuid == other.uuid

    def __repr__(self):
        return '<ImageJob %s>' % (self.uuid,)

    def __freeze__(self):
        return dict(rbuilder_url=self.rbuilder_url,
                job_data=self.job_data, uuid=self.uuid)

    @classmethod
    def __thaw__(cls, d):
        return cls(**d)

    # Scheduler helpers
    def assign_uuid(self):
        self.uuid = os.urandom(16).encode('hex')
register(ImageJob)


class _ImageJobs(object):
    name = 'ImageJobs'

    @staticmethod
    def __freeze__(jobList):
        return [freeze('ImageJob', job) for job in jobList]

    @staticmethod
    def __thaw__(jobList):
        return [thaw('ImageJob', job) for job in jobList]
register(_ImageJobs)


class _ImageNodes(object):
    name = 'ImageNodes'

    @staticmethod
    def __freeze__(nodeList):
        return [freeze('ImageNode', node) for node in nodeList]

    @staticmethod
    def __thaw__(nodeList):
        return [thaw('ImageNode', node) for node in nodeList]
register(_ImageNodes)
