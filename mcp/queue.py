# insert copyright notice

import threading
import time
import stomp

# Class level tweakable settings:
# timeOut: The time in seconds to wait on read before simply returning None
#           a timeOut of zero simply polls the queue once and returns.
#           a timeOut of None blocks forever.
# type: one of 'queue' or 'topic'
# queueLimit: control un/re-subscription as queue fills up. a queueLimit on None
#           disables this feature.

class Queue(object):
    type = 'queue'

    def __init__(self, host, port, dest, namespace = '', timeOut = 600,
                 queueLimit = None, autoSubscribe = True):
        self.lock = threading.RLock()
        self.timeOut = timeOut
        self.queueLimit = queueLimit
        self.autoSubscribe = autoSubscribe
        self.host = host
        self.port = port

        self.inbound = []
        self.connection = stomp.Connection(host, port)
        self.connection.addlistener(self)
        self.connection.start()
        if namespace:
            self.connectionName = '/'.join(('', self.type, namespace, dest))
        else:
            self.connectionName = '/'.join(('', self.type, dest))
        if self.autoSubscribe and \
                ((not self.limitedQueue) \
                     or (self.limitedQueue and self.queueLimit)):
            self._subscribe()

    def __setattr__(self, attr, val):
        if attr == 'queueLimit':
            self.limitedQueue = val is not None
        object.__setattr__(self, attr, val)

    def _subscribe(self):
        if self.connection is None:
            self.connection = stomp.Connection(self.host, self.port)
            self.connection.addlistener(self)
            self.connection.start()
        self.connection.subscribe(self.connectionName, ack='client')

    def _unsubscribe(self):
        self.connection.unsubscribe(self.connectionName)
        self.connection.disconnect()
        self.connection = None

    def receive(self, message):
        self.lock.acquire()
        try:
            if self.limitedQueue and not self.queueLimit:
                self._unsubscribe()
            else:
                self.inbound.insert(0, message.split('\n\n\n')[1].strip())
                messageLine = [x for x in message.split('\n') \
                                   if x.strip().startswith('message-id')][0]
                messageId = messageLine[messageLine.index(':') + 1:].strip()
                self.connection.ack(messageId)
                # ack before unsubscribe, or race condition ensues
                if self.limitedQueue:
                    self.queueLimit = max(self.queueLimit - 1, 0)
                    if not self.queueLimit:
                        self._unsubscribe()
        finally:
            self.lock.release()

    def send(self, message):
        self.connection.send(self.connectionName, message)

    def incrementLimit(self, increment = 1):
        self.lock.acquire()
        try:
            if self.limitedQueue:
                if not self.queueLimit:
                    self._subscribe()
                self.queueLimit += increment
        finally:
            self.lock.release()

    def setLimit(self, limit):
        self.lock.acquire()
        try:
            oldLimit = self.queueLimit
            self.queueLimit = limit
            if (self.queueLimit == 0) and (oldLimit != 0):
                self._unsubscribe()
            else:
                self._subscribe()
        finally:
            self.lock.release()

    def read(self):
        startTime = time.time()
        res = None
        runOnce = True
        while runOnce or ((self.timeOut is None) or \
                              ((time.time() - startTime) < self.timeOut)):
            runOnce = False
            self.lock.acquire()
            try:
                if len(self.inbound):
                    res = self.inbound.pop()
            finally:
                self.lock.release()
            if res:
                return res
            time.sleep(0.1)
        return None

    def disconnect(self):
        self.connection.disconnect()

class MultiplexedQueue(Queue):
    def __init__(self, host, port, dest = [], namespace = '',
                 timeOut = 600, queueLimit = None, autoSubscribe = True):
        self.lock = threading.RLock()
        self.timeOut = timeOut
        self.queueLimit = queueLimit
        self.autoSubscribe = autoSubscribe

        self.inbound = []
        self.connection = stomp.Connection(host, port)
        self.connection.addlistener(self)
        self.connection.start()
        if namespace:
            self.connectionBase = '/'.join(('', self.type, namespace))
        else:
            self.connectionBase = '/' + self.type
        self.connectionNames = []
        for d in dest:
            self.addDest(d)

    def addDest(self, dest):
        dest = self.connectionBase + '/' + dest
        self.lock.acquire()
        try:
            if dest not in self.connectionNames:
                self.connectionNames.append(dest)
        finally:
            self.lock.release()
        if self.autoSubscribe:
            self.connection.subscribe(dest, ack = 'client')

    def delDest(self, dest):
        dest = self.connectionBase + '/' + dest
        self.connection.unsubscribe(dest)
        self.lock.acquire()
        try:
            if dest in self.connectionNames:
                self.connectionNames.remove(dest)
        finally:
            self.lock.release()

    def _subscribe(self):
        if self.connection is None:
            self.connection = stomp.Connection(self.host, self.port)
            self.connection.addlistener(self)
            self.connection.start()
        self.lock.acquire()
        try:
            for dest in self.connectionNames:
                self.connection.subscribe(dest, ack = 'client')
        finally:
            self.lock.release()

    def _unsubscribe(self):
        self.lock.acquire()
        try:
            for dest in self.connectionNames:
                self.connection.unsubscribe(dest)
        finally:
            self.lock.release()
        self.connection.disconnect()
        self.connection = None

    def send(self, dest, message):
        self.connection.send(self.connectionBase + '/' + dest, message)

class Topic(Queue):
    type = 'topic'

class MultiplexedTopic(MultiplexedQueue):
    type = 'topic'
