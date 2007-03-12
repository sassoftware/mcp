#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import sys

statuses = {
    'UNKNOWN'  : -1,
    'WAITING'  : 0,
    'RUNNING'  : 100,
    'BUILT'    : 200,
    'FINISHED' : 300,
    'FAILED'   : 301,
    'ERROR'    : 301,
    'NO_JOB'   : 401
    }

sys.modules[__name__].__dict__.update(statuses)
statusNames = dict([(statuses[x[0]], x[0].capitalize().replace('_', ' ')) \
                        for x in statuses.iteritems()])

statusName = lambda status: [x[0] for x in statuses.iteritems() \
                                 if x[1] == status][0].capitalize()
