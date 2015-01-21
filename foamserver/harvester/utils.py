import os
import re
import abc
import glob
import json
import hashlib
import datetime
import logging
import itertools
from watchdog.events import FileSystemEventHandler, RegexMatchingEventHandler

logger = logging.getLogger('harvester')

class HarvesterEventHandler(RegexMatchingEventHandler):
    __metaclass__ = abc.ABCMeta

    def __init__(self,queue,cache,regexes=None):
        super(HarvesterEventHandler,self).__init__(
            regexes=self.REGEXES if regexes is None else regexes,
            ignore_directories=True)
        self.data = cache[self.TYPE]
        self.queue = queue
        self.currently_processing = set()

    def init(self,path,recursive):
        if recursive:
            for root, dirs, files in os.walk(path):
                for p in files:
                    if any(r.match(p) for r in self.regexes):
                        self.work(os.path.join(root,p))
        else:
            for p in os.listdir(path):
                if any(r.match(p) for r in self.regexes):
                    self.work(os.path.join(path,p))

    def on_modified(self,event):
        self.work(event.src_path)

    def work(self,path):
        if path in self.currently_processing:
            logger.info('already processing: {0}'.format(path))
        else:
            self.currently_processing.add(path)
            if self.test_reset(path):
                self.initialize(path)
            self.process(path)
            self.currently_processing.remove(path)

    @abc.abstractmethod
    def process(self,path):
        return

    @abc.abstractmethod
    def initialize(self,path):
        return

    @abc.abstractmethod
    def test_reset(self,path):
        return

class SystemEventHandler(HarvesterEventHandler):
    REGEXES = ['[a-z_\-A-Z]+']
    TYPE = 'system'

    def test_reset(self,path):
        if path not in self.data:
            return True

    def initialize(self,path):
        self.data[path] = {
            'initial':datetime.datetime.utcnow(),
            'hash':None,
        }

    def process(self,path):
        doc = {
            'path':path,
            'type':self.TYPE,
            'initial':self.data[path]['initial'],
        }
        hasher = hashlib.md5()
        with open(path,'r') as f:
            text = f.read()
            hasher.update(text)
            doc['text'] = text
            doc['hash'] = hasher.hexdigest()
        if doc['hash'] != self.data[path]['hash']:
            self.queue.append(doc)
            self.data[path]['hash'] = doc['hash']
        return True

class DatEventHandler(HarvesterEventHandler):
    REGEXES = ['.*\.dat']
    TYPE = 'dat'

    def test_reset(self,path):
        if path not in self.data:
            return True
        elif os.stat(path).st_size < self.data[path]['pos']:
            return True

    def initialize(self,path):
        self.data[path] = {
            'initial':datetime.datetime.utcnow(),
            'pos':0,
            'line_number':0
        }

    def process(self,path):
        d = self.data[path]
        n_min = d['line_number']
        with open(path,'r') as f:
            f.seek(d['pos'])
            data = []
            for i in range(2000):
                line = f.readline()
                if not line:
                    break
                data.append({
                    'text':line,
                    'n':i+d['line_number']})
            d['line_number'] += len(data)
            d['pos'] = f.tell()
        if len(data) > 0:
            self.queue.append(
                {'path':path,
                'type':self.TYPE,
                'n_min':n_min,
                'n_max':d['line_number'],
                'initial':d['initial'],
                'data':data})
        if os.stat(path).st_size > d['pos']:
            self.process(path)

class LogEventHandler(DatEventHandler):
    REGEXES = ['log\.']
    TYPE = 'log'
