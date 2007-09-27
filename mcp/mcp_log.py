from conary.lib.cfgtypes import CfgEnum
import logging
import signal

class HuppableFileHandler(logging.FileHandler):
    def hup(self):
        self.stream = open(self.baseFilename, 'a')

class CfgLogLevel(CfgEnum):
    validValues = ['CRITICAL', 'WARNING', 'INFO', 'DEBUG']
    def checkEntry(self, val):
        CfgEnum.checkEntry(self, val.upper())

def signalHandler(*args, **kwargs):
    log = logging.getLogger('')
    for h in log.handlers:
        if isinstance(h, HuppableFileHandler):
            h.hup()

def addRootLogger(level = None, format = None, filename = None,
        filemode = None):
    if isinstance(level, str):
        # Translate name to integer value
        level = logging.getLevelName(level.upper())
        assert isinstance(level, int)

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
