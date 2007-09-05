import logging
import signal

class HuppableFileHandler(logging.FileHandler):
    def hup(self):
        self.stream = open(self.baseFilename, 'a')

def signalHandler(*args, **kwargs):
    log = logging.getLogger('')
    for h in log.handlers:
        if isinstance(h, HuppableFileHandler):
            h.hup()

def addRootLogger(level = None, format = None, filename = None,
        filemode = None):
    log = logging.getLogger('')
    if filename:
        hdlr = HuppableFileHandler(filename, filemode or 'a')
    else:
        hdlr = logging.StreamHandler()
    fmt = logging.Formatter(format or logging.BASIC_FORMAT, None)
    hdlr.setFormatter(fmt)
    log.addHandler(hdlr)
    if level:
        log.setLevel(level)
    signal.signal(signal.SIGHUP, signalHandler)
