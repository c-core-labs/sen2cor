#!/usr/bin/env python
import os
import sys
import time
import traceback
import multiprocessing, threading, logging, sys
from logging.handlers import RotatingFileHandler

DEFAULT_LEVEL = logging.INFO
STREAM = 25 
FATAL = 50
logging.addLevelName(STREAM, " INFO")
logging.addLevelName(FATAL, "FATAL")
Logger = logging.getLoggerClass()

class SubProcessLogHandler(logging.Handler):

    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        self.queue.put(record)

class LogQueueReader(threading.Thread):

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.daemon = True

    def run(self):
        while True:
            try:
                record = self.queue.get()
                logger = logging.getLogger(record.name)
                logger.callHandlers(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except:
                traceback.print_exc(file=sys.stderr)

class UTCFormatter(logging.Formatter):
    converter = time.gmtime
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            t = time.strftime("%Y-%m-%dT%H:%M:%S", ct)
            s = "%s.%03d" % (t, record.msecs)
        return s            
    
class L2A_Logger(Logger):
    
    def __init__(self, name, fnLog = None, logLevel = DEFAULT_LEVEL, operation_mode = 'TOOLBOX'):
        Logger.__init__(self, name)
        self._logLevel = logLevel
        self._strmLevel = ' INFO'
        self._fnLog = fnLog

        if operation_mode == 'TOOLBOX' or operation_mode == None:
            if self._strmLevel == 'DEBUG':
                formatter = UTCFormatter(' %(module).12s : %(lineno)s  %(message)s')
            else:
                formatter = UTCFormatter('%(message)s')
        else:
            if self._strmLevel == 'DEBUG':
                format = '%(asctime)s  ' + os.uname()[1] +' %(process).6d %(module).13s: %(lineno)s [%(levelname)5s] %(message)s'
            else:
                format = '%(asctime)s  ' + os.uname()[1] +' %(process).6d [%(levelname)5s] %(message)s'
            formatter = UTCFormatter(fmt=format)

        strm = logging.StreamHandler(sys.stdout)
        strm.setLevel(self._strmLevel)
        strm.setFormatter(formatter)
        self.addHandler(strm)

        if fnLog:
            format = '<check>\n<inspection execution=\"%(asctime)s\" level=\"%(levelname)s\" process=\"%(process)d\" module=\"%(module)s\" function=\"%(funcName)s\" line=\"%(lineno)d\"/>\n<message contentType=\"Text\">%(message)s</message>\n</check>'
            formatter = UTCFormatter(fmt=format)
            handler = logging.FileHandler(self._fnLog,'a')
            handler.setFormatter(formatter)
            handler.setLevel(self._logLevel)
            self.addHandler(handler)
            
        self.level = getLevel(self._logLevel)
        if self._fnLog:
            self.info('logging for the main process initialized')
        
    def get_fn_log(self):
        return self._fnLog

    def set_fn_log(self, value):
        self._fnLog = value

    def del_fn_log(self):
        del self._fnLog
    fnLog = property(get_fn_log, set_fn_log, del_fn_log, "fnLog's docstring")
        
    def stream(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'STREAM'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.stream("Houston, we have a %s", "interesting problem", exc_info=1)
        """
        if self.isEnabledFor(STREAM):
            self._log(STREAM, msg, args, **kwargs)
            
def getLevel(level):
    if level == 'DEBUG':
        return logging.DEBUG
    elif level == 'INFO':
        return logging.INFO
    elif level == 'WARNING':
        logging.WARNING
    elif level == 'ERROR':
        return logging.ERROR
    elif level == 'CRITICAL':
        return logging.CRITICAL
    else:
        return logging.NOTSET