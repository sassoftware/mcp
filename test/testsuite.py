#!/usr/bin/python2.4
# -*- mode: python -*-
#
# Copyright (c) 2004-2006 rPath, Inc.
#

import bdb
import cPickle
import grp
import sys
import os
import os.path
import pwd
import socket
import re
import tempfile
import time
import types
import unittest
import __builtin__

archivePath = None
testPath = None

def context(*contexts):
    def deco(func):
        # no wrapper is needed, nor usable.
        if '_contexts' in func.__dict__:
            func._contexts.extend(contexts)
        else:
            func._contexts = list(contexts)
        return func
    return deco

class LogFilter:
    def __init__(self):
        self.records = []
        self.ignorelist = []

    def clear(self):
        from conary.lib import log
        self.records = []
        self.ignorelist = []
        log.logger.removeFilter(self)

    def filter(self, record):
        from conary.lib import log
        text = log.formatter.format(record)
        for regex in self.ignorelist:
            if regex.match(text):
                return False
        self.records.append(text)
        return False

    def ignore(self, regexp):
        self.ignorelist.append(re.compile(regexp))

    def add(self):
        from conary.lib import log
        log.logger.addFilter(self)

    def remove(self):
        from conary.lib import log
        log.logger.removeFilter(self)

    def compareWithOrder(self, records):
        """
        compares stored log messages against a sequence of messages and
        resets the filter
        """
	if self.records == None or self.records == []:
	    if records:
		raise AssertionError, "unexpected log messages"
	    return
        if type(records) is str:
            records = (records,)

	if len(records) != len(self.records):
	    raise AssertionError, "expected log message count does not match"


        for num, record in enumerate(records):
            if self.records[num] != record:
                raise AssertionError, "expected log messages do not match: '%s' != '%s'" %(self.records[num], record)
        self.records = []

    def _compare(self, desiredList, cmpFn, allowMissing = False):
        """
        compares stored log messages against a sequence of messages and
        resets the filter.  order does not matter.
        """
	if self.records == None or self.records == []:
	    if desiredList:
		raise AssertionError, "unexpected log messages"
	    return
        if not allowMissing and len(desiredList) != len(self.records):
	    raise AssertionError, "expected log message count does not match"

        matched = [ False ] * len(self.records)
        for desired in desiredList:
            match = False
            for i, record in enumerate(self.records):
                if cmpFn(record, desired):
                    match = True
                    matched[i] = True

            if not match:
                raise AssertionError, \
                        "expected log message not found: '%s'" % record

        if not allowMissing and False in matched:
            record = self.records[matched.index(False)]
            raise AssertionError, "unexpected log message found: '%s'" %record

        self.records = []

    def compare(self, records, allowMissing = False):
        if type(records) is str:
            records = (records,)

        return self._compare(records, lambda actual, desired: actual == desired,
                             allowMissing = allowMissing)

    def regexpCompare(self, records):
        if type(records) is str:
            records = (records,)

        regexps = [ re.compile(x) for x in records ]
        return self._compare(regexps,
                 lambda actual, regexp: regexp.match(actual) is not None)

_setupPath = None
def setup():
    global _setupPath
    if _setupPath:
        return _setupPath
    global testPath
    global archivePath

    invokedAs = sys.argv[0]
    if invokedAs.find("/") != -1:
        if invokedAs[0] != "/":
            invokedAs = os.getcwd() + "/" + invokedAs
        path = os.path.dirname(invokedAs)
    else:
        path = os.getcwd()

    testPath = os.path.realpath(path)
    # find the top of the test directory in the full path - this
    # sets the right test path even when testsuite.setup() is called
    # from a testcase in a subdirectory of the testPath
    if sys.modules.has_key('testsuite'):
        testPath = os.path.join(testPath, os.path.dirname(sys.modules['testsuite'].__file__))
    archivePath = testPath + '/' + "archive"
    parent = os.path.dirname(testPath)

    # ensure modules from our codebase can be loaded
    if parent not in sys.path:
        sys.path.insert(0, parent)

    # MCP specific tweaks
    import mcp_helper
    import stomp
    stomp.Connection = mcp_helper.DummyConnection
    #end MCP specific tweaks

    from conary.lib import util
    sys.excepthook = util.genExcepthook(True)

    # if we're running with COVERAGE_DIR, we'll start covering now
    from conary.lib import coveragehook

    # import debugger now that we have the path for it
    global debugger
    from conary.lib import debugger

    _setupPath = path
    return path

class Loader(unittest.TestLoader):
    suiteClass = unittest.TestSuite

    def _filterTests(self, tests):
        if not self.context:
            return
        for testCase in tests._tests[:]:
            try:
                method = testCase.__getattribute__( \
                    testCase._TestCase__testMethodName)
                contexts = method._contexts
                if self.context not in contexts:
                    tests._tests.remove(testCase)
            except AttributeError:
                tests._tests.remove(testCase)

    def loadTestsFromModule(self, module):
        """Return a suite of all tests cases contained in the given module"""
        tests = []
        for name in dir(module):
            obj = getattr(module, name)
            if (isinstance(obj, (type, types.ClassType)) and
                issubclass(obj, unittest.TestCase)):
                loadedTests = self.loadTestsFromTestCase(obj)
                self._filterTests(loadedTests)
                tests.append(loadedTests)
            if (isinstance(obj, unittest.TestSuite)):
                tests.append(obj)
        return self.suiteClass(tests)

    def loadTestsFromName(self, name, module=None):
        # test to make sure we can load what we're trying to load
        # since we can generate a better error message up front.
        if not module:
            try:
                __import__(name)
            except ImportError, e:
                print 'unable to import module %s: %s' %(name, e)
                raise
        try:
            f = unittest.TestLoader.loadTestsFromName(self, name,
                                                      module=module)
            if isinstance(f, unittest.TestSuite) and not f._tests:
                raise RuntimeError, 'no tests in %s' %name
            return f
        except AttributeError:
            # try to find a method of a test suite class that matches
            # the thing given
            for objname in dir(module):
                try:
                    newname = '.'.join((objname, name))
                    # context shouldn't apply. test cases were named directly
                    return unittest.TestLoader.loadTestsFromName(self, newname,
                                                                 module=module)
                except AttributeError:
                    pass
                except ImportError, e:
                    print 'unable to import tests from %s: %s' %(newname, e)
                    raise
        except ImportError, e:
            print 'unable to import tests from %s: %s' %(name, e)
            raise
        raise AttributeError

    def __init__(self, context = None):
        unittest.TestLoader.__init__(self)
        self.context = context

class TestCase(unittest.TestCase):

    def setUp(self):
        from conary.lib import log
        self._logLevel = log.getVerbosity()

    def tearDown(self):
        from conary.lib import log
        log.setVerbosity(self._logLevel)

    def captureOutput(self, func, *args, **kwargs):
        sys.stdout.flush()
        (fd, fn) = tempfile.mkstemp()
        os.unlink(fn)
        stdout = os.dup(sys.stdout.fileno())
        os.dup2(fd, sys.stdout.fileno())
        try:
            ret = func(*args, **kwargs)
            sys.stdout.flush()
        except:
            os.dup2(stdout, sys.stdout.fileno())
            os.close(fd)
            raise

        os.dup2(stdout, sys.stdout.fileno())
        os.close(stdout)

        # rewind and read in what was captured
        os.lseek(fd, 0, 0)
        f = os.fdopen(fd, 'r')
        s = f.read()
        # this closes the underlying fd
        f.close()

        return (ret, s)

    def discardOutput(self, func, *args, **kwargs):
        sys.stdout.flush()
        stdout = os.dup(sys.stdout.fileno())
        null = os.open('/dev/null', os.W_OK)
        os.dup2(null, sys.stdout.fileno())
        try:
            ret = func(*args, **kwargs)
            sys.stdout.flush()
        except:
            os.dup2(stdout, sys.stdout.fileno())
            os.close(null)
            raise

        os.dup2(stdout, sys.stdout.fileno())
        os.close(null)
        return ret

    def logCheck(self, fn, args, log, kwargs={}, regExp = False):
        self.logFilter.add()
        rc = fn(*args, **kwargs)
        try:
            if regExp:
                self.logFilter.regexpCompare(log)
            else:
                self.logFilter.compare(log)
        finally:
            self.logFilter.remove()
        return rc

    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)
        self.logFilter = LogFilter()
        self.owner = pwd.getpwuid(os.getuid())[0]
        self.group = grp.getgrgid(os.getgid())[0]

    def mimicRoot(self):
        self.oldgetuid = os.getuid
        self.oldgeteuid = os.geteuid
        self.oldmknod = os.mknod
        self.oldlchown = os.lchown
        self.oldchown = os.chown
        self.oldchmod = os.chmod
        self.oldchroot = os.chroot
        self.oldexecl = os.execl
        self.oldutime = os.utime
        os.getuid = lambda : 0
        os.geteuid = lambda : 0
        os.mknod = self.ourMknod
        os.lchown = self.ourChown
        os.chown = self.ourChown
        os.chmod = self.ourChmod
        os.chroot = self.ourChroot
        os.execl = self.ourExecl
        os.utime = lambda x, y: 0
        self.thisRoot = ''
        self.chownLog = []
        self.chmodLog = []
        self.mknodLog = []

    def ourChroot(self, *args):
        self.thisRoot = os.sep.join((self.thisRoot, args[0]))

    def ourExecl(self, *args):
        args = list(args)
        args[0:1] = [os.sep.join((self.thisRoot, args[0]))]
        self.oldexecl(*args)

    def ourChown(self, *args):
        self.chownLog.append(args)

    def ourMknod(self, *args):
        self.mknodLog.append(args)

    def ourChmod(self, *args):
        # we cannot chmod a file that doesn't exist (like a device node)
        # try the chmod for files that do exist
        self.chmodLog.append(args)
	try:
	    self.oldchmod(*args)
	except:
	    pass

    def realRoot(self):
        os.getuid = self.oldgetuid
        os.geteuid = self.oldgeteuid
        os.mknod = self.oldmknod
        os.lchown = self.oldlchown
        os.chown = self.oldchown
        os.chmod = self.oldchmod
        os.chroot = self.oldchroot
        os.execl = self.oldexecl
        os.utime = self.oldutime
        self.chownLog = []

class TestTimer(object):
    def __init__(self, file, testSuite):
        self.startAll = time.time()
        if os.path.exists(file):
            try:
                self.times = cPickle.load(open(file))
            except Exception, msg:
                print "error loading test times:", msg
                self.times = {}
        else:
            self.times = {}
        self.file = file

        self.toRun = set()
        testSuites = [testSuite]
        while testSuites:
            testSuite = testSuites.pop()
            if not isinstance(testSuite, unittest.TestCase):
                testSuites.extend(x for x in testSuite)
            else:
                self.toRun.add(testSuite.id())

    def startTest(self, test):
        self.testStart = time.time()
        self.testId = test.id()

    def stopTest(self, test):
        id = self.testId
        self.toRun.discard(id)

    def testPassed(self):
        id = self.testId
        thisTime = time.time() - self.testStart
        avg, times = self.times.get(id, [0, 0])
        avg = ((avg * times) + thisTime) / (times + 1.0)
        times = min(times+1, 3)
        self.times[id] = [avg, times]
        self.store(self.file)

    def estimate(self):
        left =  sum(self.times.get(x, [1])[0] for x in self.toRun)
        passed = time.time() - self.startAll
        return  passed, passed + left

    def store(self, file):
        cPickle.dump(self.times, open(file, 'w'))


class SkipTestException(Exception):
    def __init__(self, args=''):
        self.args = args

class SkipTestResultMixin:
    def __init__(self):
        self.skipped = []

    def checkForSkipException(self, test, err):
        # because of the reloading of modules that occurs when
        # running multiple tests, no guarantee about the relation of
        # this SkipTestException class to the one run in the
        # actual test can be made, so just check names
        if err[0].__name__ == 'SkipTestException':
            self.addSkipped(test, err)
            return True

    def addSkipped(self, test, err):
        self.skipped.append(test)

    def __repr__(self):
        return ("<%s run=%i errors=%i failures=%i skipped=%i>" %
                (unittest._strclass(self.__class__), self.testsRun,
                 len(self.errors), len(self.failures),
                 len(self.skipped)))


class TestCallback:

    def _message(self, msg):
        if self.oneLine:
            self.out.write("\r")
            self.out.write(msg)
            if len(msg) < self.last:
                i = self.last - len(msg)
                self.out.write(" " * i + "\b" * i)
            self.out.flush()
            self.last = len(msg)
        else:
            self.out.write(msg)
            self.out.write('\n')
            self.out.flush()

    def __del__(self):
        if self.last and self.oneLine:
            self._message("")
            print "\r",
            self.out.flush()

    def clear(self):
        if self.oneLine:
            self._message("")
            print "\r",

    def __init__(self, f = sys.stdout, oneLine = True):
        self.oneLine = oneLine
        self.last = 0
        self.out = f

    def totals(self, run, passed, failed, errored, skipped, total,
               timePassed, estTotal, test=None):
        totals = (failed +  errored, skipped, timePassed / 60,
                  timePassed % 60, estTotal / 60, estTotal % 60, run, total)
        msg = 'Fail: %s Skip: %s - %0d:%02d/%0d:%02d - %s/%s' % totals

        if test:
            # append end of test to message
            id = test.id()
            if self.oneLine:
                cutoff = max((len(id) + len(msg)) - 76, 0)
            else:
                cutoff = 0
            msg = msg + ' - ' + id[cutoff:]

        if test or self.oneLine:
            self._message(msg)


class SkipTestTextResult(unittest._TextTestResult, SkipTestResultMixin):

    def __init__(self, *args, **kw):
        test = kw.pop('test')
        self.debug = kw.pop('debug', False)
        self.useCallback = kw.pop('useCallback', True)
        oneLine = kw.pop('oneLine', False)
        self.passedTests = 0
        self.failedTests = 0
        self.erroredTests = 0
        self.skippedTests = 0
        self.total = test.countTestCases()
        self.callback = TestCallback(oneLine=oneLine)
        unittest._TextTestResult.__init__(self, *args, **kw)
        SkipTestResultMixin.__init__(self)
        self.timer = TestTimer('.times', test)
        self.stderr = os.fdopen(os.dup(sys.stderr.fileno()), 'w', 0)

    def addSkipped(self, test, err):
        self.skippedTests += 1
        SkipTestResultMixin.addSkipped(self, test, err)
        if self.useCallback:
            self.callback.clear()
            if err[1].args:
                msg = '(%s)' %err[1].args
            else:
                msg = ''
            print 'SKIPPED:', test.id(), msg

    def addError(self, test, err):
        if isinstance(err[1], bdb.BdbQuit):
            raise KeyboardInterrupt

        if self.checkForSkipException(test, err):
            return

        unittest.TestResult.addError(self, test, err)
        if self.useCallback:
            self.callback.clear()
        else:
            print
        desc = self._exc_info_to_string(err, test)
        self.printErrorList('ERROR', [(test, desc)])

        if self.debug:
            debugger.post_mortem(err[2], err[1], err[0])

        self.erroredTests += 1


    def addFailure(self, test, err):
        unittest.TestResult.addFailure(self, test, err)
        if self.useCallback:
            self.callback.clear()
        else:
            print
        desc = self._exc_info_to_string(err, test)
        self.printErrorList('FAILURE', [(test, desc)])

        if self.debug:
            debugger.post_mortem(err[2], err[1], err[0])

        self.failedTests += 1

    def addSuccess(self, test):
        self.timer.testPassed()
        self.passedTests += 1

        if not self.useCallback:
            unittest._TextTestResult.addSuccess(self, test)

    def startTest(self, test):
        unittest._TextTestResult.startTest(self, test)
        self.timer.startTest(test)
        self.printTotals(test)

    def stopTest(self, test):
        unittest._TextTestResult.stopTest(self, test)
        self.timer.stopTest(test)
        self.printTotals()


    def printTotals(self, test=None):
        if self.useCallback:
            timePassed, totalTime = self.timer.estimate()
            self.callback.totals(self.testsRun, self.passedTests,
                                 self.failedTests,
                                 self.erroredTests, self.skippedTests,
                                 self.total, timePassed, totalTime, test)

    def printErrorList(self, flavour, errors):
        for test, err in errors:
            self.stderr.write(self.separator1)
            self.stderr.write('\n')
            self.stderr.write("%s: %s" % (flavour,self.getDescription(test)))
            self.stderr.write('\n')
            self.stderr.write(self.separator2)
            self.stderr.write('\n')
            self.stderr.write(str(err))
            self.stderr.write('\n')

class DebugTestRunner(unittest.TextTestRunner):
    def __init__(self, *args, **kwargs):
        self.debug = kwargs.pop('debug', False)
        self.useCallback = kwargs.pop('useCallback', False)
        self.oneLine = kwargs.pop('oneLine', True)
        unittest.TextTestRunner.__init__(self, *args, **kwargs)

    def run(self, test):
        self.test = test
        result = self._makeResult()
        startTime = time.time()
        test(result)
        stopTime = time.time()
        timeTaken = stopTime - startTime
        self.stream.writeln('\n' + result.separator2)
        run = result.testsRun
        self.stream.writeln("Ran %d test%s in %.3fs" %
                            (run, run != 1 and "s" or "", timeTaken))
        return result

    def _makeResult(self):
        return SkipTestTextResult(self.stream, self.descriptions,
                                  0, test=self.test,
                                  debug=self.debug,
                                  useCallback=self.useCallback,
                                  oneLine=self.oneLine)


_individual = False

def isIndividual():
    global _individual
    return _individual

def main(*args, **keywords):
    from conary.lib import util
    sys.excepthook = util.genExcepthook(True)

    global _individual
    _individual = True
    if '--coverage' in sys.argv:
        runCoverage()
        return

    statFile = None
    if '--stat-file' in sys.argv:
        argLoc = sys.argv.index('--stat-file')
        statFile = sys.argv[argLoc + 1]
        del sys.argv[argLoc+1]
        del sys.argv[argLoc]

    context = None
    if '--context' in sys.argv:
        argLoc = sys.argv.index('--context')
        context = sys.argv[argLoc + 1]
        sys.argv.remove('--context')
        sys.argv.pop(argLoc)

    loader = Loader(context = context)

    oneLine = True
    dots = False

    if '-v' in sys.argv:
        oneLine = False
        sys.argv.remove('-v')

    if '--dots' in sys.argv:
        sys.argv.remove('--dots')
        dots = True

    if '--debug' in sys.argv:
        debug = True
        sys.argv.remove('--debug')
    else:
        debug=False

    # output to stdout, not stderr.  reopen because we do some mucking
    # with sys.stdout. Run unbuffered for immediate output.
    stream = os.fdopen(os.dup(sys.stdout.fileno()), 'w', 0)
    runner = DebugTestRunner(debug=debug, useCallback=not dots,
                             oneLine=oneLine, stream=stream)

    try:
        unittest.main(testRunner=runner, testLoader=loader, *args, **keywords)
    finally:
        if statFile:
            outputStats(runner, open(statFile, 'w'))
            import epdb
            epdb.st()

def outputStats(results, outFile):
    outFile.write('''\
tests run: %s
skipped:   %s
failed:    %s
''' % (results.testsRun, results.skippedTests,
      (results.erroredTests + results.failedTests)))


def runCoverage():
    import coveragewrapper
    environ = coveragewrapper.getEnviron()
    idx = sys.argv.index('--coverage')

    if '--stat-file' in sys.argv:
        argLoc = sys.argv.index('--stat-file')
        statFile = sys.argv[argLoc + 1]
    else:
        statFile = None
    doAnnotate = '--no-annotate' not in sys.argv
    if not doAnnotate: sys.argv.remove('--no-annotate')
    doReport = '--no-report' not in sys.argv
    if not doReport: sys.argv.remove('--no-report')

    baseDirs = [environ['mcp']]
    if len(sys.argv) > idx+1 and not sys.argv[idx+1].startswith('--'):
        filesToCover = sys.argv[idx+1].split(',')
        del sys.argv[idx + 1]
        if filesToCover == ['']:
            filesToCover = []
    else:
        filesToCover = []
    del sys.argv[idx]

    files, notExists = coveragewrapper.getFilesToAnnotate(baseDirs,
                                                          filesToCover)
    if notExists:
        raise RuntimeError, 'no such file(s): %s' % ' '.join(notExists)

    cw = coveragewrapper.CoverageWrapper(environ['coverage'],
                                         os.getcwd() + '/.coverage',
                                         os.getcwd() + '/annotate')
    cw.reset()
    cw.execute(' '.join(sys.argv))
    if doReport:
        cw.displayReport(files)
    if doAnnotate:
        from conary.lib import util
        util.mkdirChain('annotate')
        cw.writeAnnotatedFiles(files)

    if statFile:
        print 'Writing coverage stats (this could take a while)...'
        open(statFile, 'a').write('''\
lines:   %s
covered: %s
percent covered: %s
''' % cw.getTotals(files))
    return


if __name__ == '__main__':
    tests = []
    topdir = os.path.join(os.getcwd(), os.path.dirname(sys.argv[0]))
    topdir = os.path.normpath(topdir)
    cwd = os.getcwd()
    if topdir not in sys.path:
        sys.path.insert(0, topdir)
    if cwd != topdir and cwd not in sys.path:
        sys.path.insert(0, cwd)
    setup()

    from conary.lib import util
    from conary.lib import debugger
    sys.excepthook = util.genExcepthook(True)

    if '--coverage' in sys.argv:
        runCoverage()
        sys.exit(0)

    statFile = None
    if '--stat-file' in sys.argv:
        argLoc = sys.argv.index('--stat-file')
        statFile = sys.argv[argLoc + 1]
        del sys.argv[argLoc+1]
        del sys.argv[argLoc]

    debug = False
    dots = False
    if '--debug' in sys.argv:
	debug = True
	sys.argv.remove('--debug')

    oneLine = True
    if '-v' in sys.argv:
        oneLine = False
        sys.argv.remove('-v')

    if '--dots' in sys.argv:
        dots = True
        sys.argv.remove('--dots')

    context = None
    if '--context' in sys.argv:
        argLoc = sys.argv.index('--context')
        context = sys.argv[argLoc + 1]
        sys.argv.remove('--context')
        sys.argv.pop(argLoc)

    if sys.argv[1:]:
	tests = []
	for s in sys.argv[1:]:
	    # strip the .py
            if s.endswith('.py'):
                s = s[:-3]
            if topdir != cwd:
                s = '%s.%s' %(cwd[len(topdir) + 1:].replace('/', '.'), s)
            tests.append(s)
    else:
	for (dirpath, dirnames, filenames) in os.walk(topdir):
	    for f in filenames:
		if f.endswith('test.py') and not f.startswith('.'):
		    # turn any subdir into a dotted module string
		    d = dirpath[len(topdir) + 1:].replace('/', '.')
		    if d:
			# if there's a subdir, add a . to delineate
			d += '.'
		    # strip off .py
		    tests.append(d + f[:-3])

    loader = Loader(context = context)
    suite = unittest.TestSuite()

    for test in tests:
        testcase = loader.loadTestsFromName(test)
        suite.addTest(testcase)

    # output to stdout, not stderr.  reopen because we do some mucking
    # with sys.stdout. Run unbuffered for immediate output.
    stream = os.fdopen(os.dup(sys.stdout.fileno()), 'w', 0)
    runner = DebugTestRunner(debug=debug, useCallback=not dots,
                             oneLine=oneLine, stream=stream)
    results = runner.run(suite)

    if statFile:
        outputStats(results, open(statFile, 'w'))

    sys.exit(not results.wasSuccessful())
