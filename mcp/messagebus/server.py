#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import logging
from rmake.multinode.server import messagebus

bus = messagebus.MessageBus('::', 50900, '/tmp/mb.log', '/tmp/mb.messages')
bus._logger.logger.handlers = [logging.StreamHandler()]
bus.serve_forever()
