# insert copyright notice

import logging
import socket
import stomp
import sys
import threading
import time
import traceback

import mcp_error


def logErrors(func):
    def wrapper(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except:
            # receive is not atomic, we cannot recover
            exc, e, bt = sys.exc_info()
            logging.error("CRITICAL ERROR: Stomp receive thread has crashed!")
            logging.error(e.__class__.__name__ + ": " + str(e))
            logging.error('\n'.join(traceback.format_tb(bt)))
            raise
    return wrapper


# Class level tweakable settings:
# timeOut: The time in seconds to wait on read before simply returning None
#           a timeOut of zero simply polls the queue once and returns.
#           a timeOut of None blocks forever.
# type: one of 'queue' or 'topic'
# queueLimit: control un/re-subscription as queue fills up. a queueLimit of None
#           disables this feature.
# autoSubscribe: subscribe to a queue immediately or not. this is generally used
#           if you want to send on a queue only, but never listen.
# namespace: an extension of queue names. eg: /topic/_namespace_/topic_name

class Queue(stomp.ConnectionListener):
    type = 'queue'
    ack = 'client'

    def __init__(self, host, port, dest, namespace = '', timeOut = 600,
                 queueLimit = None, autoSubscribe = True,
                 maxConnectionAttempts = 0):
        self.lock = threading.RLock()
        self.timeOut = timeOut
        self.queueLimit = queueLimit
        self.autoSubscribe = autoSubscribe
        self.maxConnectionAttempts = maxConnectionAttempts
        self.host = host
        self.port = port
        self.connection = None

        self.inbound = []

        if namespace:
            self.connectionName = '/'.join(('', self.type, namespace, dest))
        else:
            self.connectionName = '/'.join(('', self.type, dest))

        self._connectToBroker()

    def __setattr__(self, attr, val):
        if attr == 'queueLimit':
            self.limitedQueue = val is not None
        object.__setattr__(self, attr, val)

    def _connectToBroker(self):
        if self.connection and self.connection.is_connected():
            return

        try:
            self.connection = stomp.Connection([(self.host, self.port),],
                    prefer_localhost = False,
                    max_connection_attempts=self.maxConnectionAttempts)
            self.connection.add_listener(self)
            self.connection.start()
            self.connection.connect()
            self._handleSubscriptions()
        except socket.error, e_value:
            raise mcp_error.NetworkError(str(e_value))
        except stomp.MaxConnectionAttemptsExceededException:
            raise mcp_error.NetworkError("Failed to connect to the stomp broker")

    def _handleSubscriptions(self):
        if self.autoSubscribe and \
                ((not self.limitedQueue) \
                     or (self.limitedQueue and self.queueLimit)):
            self._subscribe()

    def _subscribe(self):
        self._connectToBroker()
        self.connection.subscribe(destination=self.connectionName, ack = self.ack)

    def _unsubscribe(self):
        self.connection.unsubscribe(destination=self.connectionName)
        self.disconnect()

    @logErrors
    def on_message(self, headers, message):
        self.lock.acquire()
        try:
            if self.limitedQueue and not self.queueLimit:
                self._unsubscribe()
            else:
                self.inbound.insert(0, message)
                if self.ack == 'client':
                    self.connection.ack(message_id=headers['message-id'])
                # ack before unsubscribe, or race condition ensues
                if self.limitedQueue:
                    self.queueLimit = max(self.queueLimit - 1, 0)
                    if not self.queueLimit:
                        self._unsubscribe()
        finally:
            self.lock.release()

    def on_disconnected(self):
        self.lock.acquire()
        try:
            self.connection = None
        finally:
            self.lock.release()

    def send(self, message):
        self._connectToBroker()
        self.lock.acquire()
        try:
            self.connection.send(message, destination=self.connectionName)
        finally:
            self.lock.release()

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
            count = len(self.inbound)
            self.queueLimit = max(0, limit - count)
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
        self.lock.acquire()
        try:
            if self.connection:
                # stomp.py has some sort of race condition where a disconnect
                # can fail due to the socket being cleared between this check
                # and the disconnect call, even though we guard the
                # on_disconnect handler. Unfortunately, this means eating
                # errors.
                try:
                    if self.connection._Connection__socket:
                        self.connection.disconnect()
                # AttributeError: 'NoneType' object has no attribute 'shutdown'
                except AttributeError:
                    pass
            self.connection = None
        finally:
            self.lock.release()

    def __del__(self):
        self.disconnect()

class MultiplexedQueue(Queue):
    def __init__(self, host, port, dest = [], namespace = '',
                 timeOut = 600, queueLimit = None, autoSubscribe = True,
                 maxConnectionAttempts = 0):

        Queue.__init__(self, host, port, '', namespace,
                timeOut, queueLimit, autoSubscribe, maxConnectionAttempts)
        del self.connectionName

        if namespace:
            self.connectionBase = '/'.join(('', self.type, namespace))
        else:
            self.connectionBase = '/' + self.type

        self.connectionNames = []

        self._connectToBroker()

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
            self.connection.subscribe(destination=dest, ack = self.ack)

    def delDest(self, dest):
        dest = self.connectionBase + '/' + dest
        self.connection.unsubscribe(destination=dest)
        self.lock.acquire()
        try:
            if dest in self.connectionNames:
                self.connectionNames.remove(dest)
        finally:
            self.lock.release()

    def _subscribe(self):
        self._connectToBroker()
        self.lock.acquire()
        try:
            for dest in self.connectionNames:
                self.connection.subscribe(destination=dest, ack = self.ack)
        finally:
            self.lock.release()

    def _unsubscribe(self):
        self.lock.acquire()
        try:
            for dest in self.connectionNames:
                self.connection.unsubscribe(destination=dest)
        finally:
            self.lock.release()
        self.disconnect()

    def _handleSubscriptions(self):
        pass # not used in Multiplexed versions of this class

    def send(self, dest, message):
        self._connectToBroker()
        self.lock.acquire()
        try:
            self.connection.send(message, destination=self.connectionBase + '/' + dest)
        finally:
            self.lock.release()

class Topic(Queue):
    type = 'topic'
    ack = 'auto'

class MultiplexedTopic(MultiplexedQueue):
    type = 'topic'
    ack = 'auto'
