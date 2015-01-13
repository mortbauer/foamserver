import os
import re
import glob
import json
import hashlib
import datetime
import logging
import pyparsing
from watchdog.events import FileSystemEventHandler, RegexMatchingEventHandler

logger = logging.getLogger('harvester')
pline = pyparsing.OneOrMore(pyparsing.nestedExpr())
phead = pyparsing.OneOrMore(pyparsing.Word(pyparsing.alphas)+pyparsing.nestedExpr())
class SystemEventHandler(RegexMatchingEventHandler):
    ignore_directories = True

    def __init__(self,queue,path='system',data=None):
        super(SystemEventHandler,self).__init__(
            regexes=[os.path.join(path,'[a-z_\-A-Z]+')],
            ignore_directories=True
        )
        self.data = {} if data is None else data
        self.queue = queue
        for rel_path in os.listdir(path):
            fpath = os.path.join(path,rel_path)
            if os.path.isfile(fpath):
                self.get_data(fpath)

    def get_data(self,fpath):
        doc = {
            'path':fpath,
            'type':'system',
            'timestamp':datetime.datetime.utcnow(),
            'is_new':True, #always mark system data as new
        }
        hasher = hashlib.md5()
        with open(fpath,'r') as f:
            text = f.read()
            hasher.update(text)
            doc['text'] = text
            doc['hash'] = hasher.hexdigest()
        if fpath not in self.data or doc['hash'] != self.data[fpath]['hash']:
            self.queue.append(doc)
            self.data[fpath] = {'hash':doc['hash']}
        return True

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

    def __init__(self,queue,path,data=None):
        super(DatEventHandler,self).__init__(
            ignore_directories=True,regexes=['.*.dat'])
        self.data = {} if data is None else data
        self.queue = queue
        for fpath in glob.glob(os.path.join(path,'**/**/*.dat')):
            self.update_data(fpath)

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
                    elif len(line.split())>2:
                        if not 'name' in d['meta']:
                            d['meta']['head'] = line.split()[1:]
                        else:
                            l = line.split(' ',2)[1:]
                            l1 = phead.parseString(l[1]).asList()
                            h = [l[0]]
                            for x in range(len(l1))[::2]:
                                for i in range(3):
                                    h.append('{0}_{1}'.format(l1[x],l1[x+1][i]))
                            d['meta']['head'] = h
                        data = {x:[] for x in d['meta']['head']}
                    else:
                        d['meta']['name'] = line.lstrip()[1:]
                else:
                    if not 'name' in d['meta']:
                        dline = [float(x) for x in line.split()]
                    else:
                        l = line.split('\t',1)
                        l1 = pline.parseString(l[1]).asList()
                        dline = [l[0]]
                        for x in l1:
                            for i in range(3):
                                dline.append([float(k) for k in x[i]])
                    for i,col in enumerate(d['meta']['head']):
                        data[col].append(dline[i])

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

