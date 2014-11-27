import os
import re
import glob
import json
import hashlib
import datetime
from dictdiffer import DictDiffer
from watchdog.events import FileSystemEventHandler, RegexMatchingEventHandler
from PyFoam.RunDictionary.ParsedParameterFile import FoamFileParser, FoamStringParser
from PyFoam.Basics import DataStructures

class PyFoamDictDiffer(DictDiffer):
    def _diff_DictProxy(self,first,second,node):
        return self._diff_dict(first,second,node)
    def _diff_ListProxy(self,first,second,node):
        return self._diff_list(first,second,node)
    def _diff_BoolProxy(self,first,second,node):
        return self._diff_generic(first,second,node)

class PyFoamDictEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,DataStructures.BoolProxy):
            return json.JSONEncoder.encode(self, obj.val)
        elif isinstance(obj,DataStructures.Vector):
            return json.JSONEncoder.encode(self, obj.vals)
        elif isinstance(obj,DataStructures.Field):
            return str(obj)
        elif isinstance(obj,DataStructures.Dimension):
            return json.JSONEncoder.encode(self, obj.dims)
        elif isinstance(obj,datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)

class SystemEventHandler(RegexMatchingEventHandler):
    ignore_directories = True

    def __init__(self,queue,path='system',data=None,logger=None):
        super(SystemEventHandler,self).__init__(
            regexes=[os.path.join(path,'[a-z_\-A-Z]+')],
            ignore_directories=True
        )
        self._logger = logger
        self.data = {} if data is None else data
        self.differ = PyFoamDictDiffer()
        self.queue = queue
        for rel_path in os.listdir(path):
            fpath = os.path.join(path,rel_path)
            self.get_data(fpath)

    def get_data(self,fpath):
        d = {}
        hasher = hashlib.md5()
        stat = os.stat(fpath)
        with open(fpath,'r') as f:
            text = f.read()
            hasher.update(text)
            try:
                parser = FoamStringParser(text)
            except Exception as e:
                self.log('error','FoamStringParser failed because of: {0}'.format(e))
                try:
                    parser = FoamFileParser(text)
                except Exception as e:
                    self.log('error','FoamFileParser failed because of: {0}'.format(e))
                    raise Exception('unparseable file, {0}'.format(fpath))
            if parser.header is not None:
                d['header'] = parser.header
            d['data'] = parser.data
            d['hash'] = hasher.hexdigest()
        doc = {
            'path':fpath,
            'type':'system',
            'timestamp':datetime.datetime.utcnow(),
        }
        if fpath not in self.data:
            doc['doc'] = d['data']
            self.queue.append(doc)
        elif d['hash'] != self.data[fpath]['hash']:
            doc['diffs'] = list(self.differ.diff(
                self.data[fpath]['data'],d['data']))
            if len(doc['diffs']) > 0:
                self.queue.append(doc)
        # update my data
        self.data[fpath] = d
        return True

    def log(self,level,msg):
        if self._logger is not None:
            getattr(self._logger,level)(msg)

    def on_modified(self,event):
        self.get_data(event.src_path)

class LogEventHandler(RegexMatchingEventHandler):

    def __init__(self,queue,path='./',data=None):
        super(LogEventHandler,self).__init__(
            regexes=[os.path.join(path,x) for x in [
                'log\.[a-zA-Z.0-9]+$','slurm-[a-zA-Z0-9]+\.out','FOAM\.o[0-9]+']])
        self.data = {} if data is None else data
        self.queue = queue
        for rel_path in os.listdir(path):
            for pattern in self.regexes:
                fpath = os.path.join(path,rel_path)
                if pattern.match(fpath):
                    self.process_lines(fpath)
                    break

    def process_lines(self,fpath):
        is_new = False
        stat = os.stat(fpath)
        if not fpath in self.data or stat.st_size < self.data[fpath]['last_pos']:
            self.data[fpath] = d = {'last_pos':0}
            is_new = False
        else:
            d = self.data[fpath]
        with open(fpath,'r') as f:
            f.seek(d['last_pos'])
            data = f.readlines()
            if len(data) > 0:
                self.queue.append(
                    {'path':fpath,
                     'type':'log',
                     'is_new':is_new,
                     'ctime':datetime.datetime.fromtimestamp(stat.st_ctime),
                     'timestamp':datetime.datetime.utcnow(),
                     'loglines':data})
            d['last_pos'] = f.tell()


    def on_modified(self,event):
        self.process_lines(event.src_path)

class DatEventHandler(RegexMatchingEventHandler):

    def __init__(self,queue,path,data=None,logger=None):
        super(DatEventHandler,self).__init__(
            ignore_directories=True,regexes=['.*.dat'])
        self._logger = logger
        self.data = {} if data is None else data
        self.queue = queue
        for fpath in glob.glob(os.path.join(path,'**/**/*.dat')):
            self.update_data(fpath)

    def log(self,level,msg):
        if self._logger is not None:
            getattr(self._logger,level)(msg)

    def update_data(self,fpath):
        stat = os.stat(fpath)
        is_new = False
        if fpath not in self.data or stat.st_size < self.data[fpath]['last_pos']:
            d = self.data[fpath] = {'line':0,'last_pos':0,'meta':{}}
            is_new = True
        elif stat.st_size > self.data[fpath]['last_pos']:
            d = self.data[fpath]
            if 'head' in d['meta']:
                data = {x:[] for x in d['meta']['head']}
        else:
            return False
        with open(fpath,'r') as f:
            f.seek(d['last_pos'])
            for line in f:
                if line.lstrip().startswith('#'):
                    if ':' in line:
                        label,val = line.lstrip()[1:].split(':')
                        d['meta'][label.strip()] = val.strip()
                    else:
                        d['meta']['head'] = line.split()[1:]
                        data = {x:[] for x in d['meta']['head']}
                else:
                    try:
                        dline = [float(x) for x in line.split()]
                        for i,col in enumerate(d['meta']['head']):
                            data[col].append(dline[i])
                    except ValueError:
                        self.log('error','failed to get dat: {0}'.format(line))
                d['line'] += 1
            d['last_pos'] = f.tell()
        self.queue.append(
            {'path':fpath,
             'type':'dat',
             'is_new':is_new,
             'ctime':datetime.datetime.fromtimestamp(stat.st_ctime),
             'timestamp':datetime.datetime.utcnow(),
             'meta':d['meta'],
             'data':data})
        return True

    def on_modified(self,event):
        d = self.update_data(event.src_path)

