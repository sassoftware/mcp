#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import sys

statuses = {
    'UNKNOWN'  : -1,
    'BUILDING' : 100,
    'STARTED'  : 200,
    'IDLE'     : 200,
    'ACTIVE'   : 201,
    'OFFLINE'  : 300,
    }

sys.modules[__name__].__dict__.update(statuses)
statusNames = dict([(statuses[x[0]], x[0].capitalize().replace('_', ' ')) \
                        for x in statuses.iteritems()])
