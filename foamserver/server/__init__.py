import os
import sys
import zmq
import time
import json
import yaml
import click
import signal
import logging
import pymongo
import tornado
import threading
import datetime
import traceback
import collections
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from zmq.eventloop import ioloop, zmqstream

fmt = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.ERROR,format=fmt)
logger = logging.getLogger('foamserver_server')
logger.setLevel(logging.INFO)
fm = logging.Formatter(fmt)

class FoamServerException(Exception):
    pass

class BaseProcessor(object):
    def __call__(self,doc,*args):
        try:
            return self.TYPE, doc, self.process(doc,*args)
        except Exception as e:
            print(traceback.format_exc())

class DatPostProcessor(BaseProcessor):
    TYPE = 'dat_post'
    def process(self,postdoc,docs):
        ncols = len(docs[0]['data'][-1]['data'])
        data = [[] for i in range(ncols)]
        push = {}
        result = {
            '$max':{'n_max':docs[-1]['n_max']},
            '$min':{'n_min':docs[0]['n_min']},
            '$set':{
                'file':docs[0]['file'],
                'name':docs[0]['name'],
                'starttime':docs[0]['starttime'],
            },
        }
        # the docs need to be in sorted order
        for doc in docs:
            # the lines in the docs are anyways in sorted order
            for line in doc['data']:
                if line['n'] <2:
                    continue
                elif line['n'] == 2:
                    result['$set']['head'] = head = line['data']
                else:
                    for i in range(ncols):
                        data[i].append(line['data'][i])

        head = postdoc.get('head',result['$set'].get('head'))
        if head is None:
            logger.error('no head for {0}'.format(postdoc['_id']))
        else:
            for i,x in enumerate(head):
                push['data.{0}'.format(x)] = {'$each':data[i]}
        if len(push):
            result['$push'] =push
        result['$set']['modified'] =datetime.datetime.utcnow()
        return result

class LogPostProcessor(BaseProcessor):
    TYPE = 'log_post'
    def process(self,postdoc,docs):
        res = {
            'iterations':{'time':[],'initial residual':{}},
            'outer_iterations':{},
            'corrector':{},
        }
        initial = {}
        n_iter = 0
        result = {
            '$max':{'n_max':docs[-1]['n_max']},
            '$min':{'n_min':docs[0]['n_min']},
        }

        for doc in docs:
            for line in doc['data']:
                if not 'data' in line:
                    continue
                data = line['data']
                if 'time' in data:
                    res['iterations']['time'].append(data['time'])
                elif 'variable' in data:
                    if data['variable'] not in initial:
                        initial[data['variable']] = []
                    initial[data['variable']].append(data['initial residual'])
        push = {}
        for x in initial:
            push['data.iterations.initial residual.{0}'.format(x)] = {'$each':initial[x]}
        if len(push):
            result['$push'] = push
        result['$set'] = {'modified':datetime.datetime.utcnow()}
        return result

class LogProcessor(BaseProcessor):
    TYPE = 'log'
    def process(self,doc):
        doc['processed'] = True
        for line in doc['data']:
            self.process_line(line)
        return doc

    def process_line(self,line):
        text = line['text']
        if text.startswith('Time ='):
            line['data'] = {
                'time':float(text.split(' = ')[1]),
            }
        elif 'Solving for ' in text:
            fields = text.split(',')
            solver, variable = fields[0].split(':')
            line['data'] = {
                'variable':variable.split()[-1],
                'solver':solver,
                'n_iter':int(fields[3].split()[-1]),
            }
            r = fields[1].split(' = ')
            line['data'][r[0].strip().lower()] = float(r[1])
            r = fields[2].split(' = ')
            line['data'][r[0].strip().lower()] = float(r[1])
        elif 'continuity errors' in text:
            line['data'] = {}
            for item in text.split(':')[1].split(','):
                r = item.split(' = ')
                line['data'][r[0].strip()] = float(r[1])
        elif text.startswith('ExecutionTime'):
            line['data'] = {}
            for item in text.split('  '):
                key,rest = item.split(' = ')
                value,unit = rest.split()
                line['data'][key.strip()] = {'value':float(value),'unit':unit}

class SystemProcessor(BaseProcessor):
    TYPE = 'system'
    def process(self,doc):
        doc['processed'] = True
        return doc

class DatProcessor(BaseProcessor):
    TYPE = 'dat'
    def process(self,doc):
        path_pieces = doc['path'].split('/')
        doc['file'] = path_pieces[-1]
        doc['starttime'] = path_pieces[-2]
        doc['name'] = path_pieces[-3]
        doc['processed'] = True
        if 'forces' in path_pieces[-1]:
            processor = self.process_force_line
        else:
            processor = self.process_scalar_line
        for line in doc['data']:
            processor(line)
        return doc

    def process_force_line(self,line):
        n = line['n']
        text = line['text']
        line['data'] = res = []
        if n == 2:
            comment,_time,fields = text.split(' ',2)
            res.append(_time)
            for main_field in fields.strip().split(') '):
                key,minor_fields = main_field.split('(')
                for minor in minor_fields.split():
                    res.append('{0}_{1}'.format(key,minor))
        elif n > 2:
            _time,_fields = text.strip().split('\t')
            res.append(float(_time))
            for main_field in _fields.split(') '):
                sub_res = []
                for x in main_field.strip('()').split():
                    sub_res.append(float(x))
                res.append(sub_res)


    def process_scalar_line(self,line):
        n = line['n']
        text = line['text']
        line['data'] = res = []
        if n == 2:
            line['data'] = text.strip('\n# ').split('\t',2)
        elif n > 2:
            for x in text.strip('\n#').split('\t',2):
                res.append(float(x))


class FoamServer(object):
    SERVER_PUSH_PORT = 5051
    SERVER_PULL_PORT = 5052
    SLEEP_TIME = 3

    def __init__(self,conffile=None,debug=False,loglevel='error'):
        try:
            self.client = pymongo.MongoClient('localhost',27017)
        except pymongo.errors.ConnectionFailure:
            raise FoamServerException('couldn\'t connect to mongodb on localhost:27017')
        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(loglevel.upper())
        self.load_conf(conffile)
        self.add_logging_file_handler()
        self.db = db = self.client['ventilator']
        self._stop = False
        self.context = zmq.Context()
        self._loop = ioloop.IOLoop.instance()
        self._postpro_rq = collections.deque()
        self._process_rq = collections.deque()
        self._postpro_queue = collections.deque()
        self.processors = {
            'dat':DatProcessor(),
            'log':LogProcessor(),
            'system':SystemProcessor(),
        }
        self.post_processors = {
            'dat_post':DatPostProcessor(),
            'log_post':LogPostProcessor(),
        }
        self.executor = ProcessPoolExecutor(max_workers=4)

        self._process_control_loop = ioloop.PeriodicCallback(
            self._process_control_task,self.SLEEP_TIME*1e3,self._loop)
        self._process_control_loop.start()

    def _update_db_and_queu(self,future):
        _type,doc,res = future.result()
        path = doc['_id']['path'] if _type.endswith('_post') else doc['path']
        try:
            ok = self.db[_type].update({'_id':doc['_id']},res)
        except Exception as e:
            print(traceback.format_exc())
        else:
            if ok['nModified']:
                logger.info('update {0}: {1}'.format(_type,path))
                if _type in ['dat','log']:
                    self._postpro_queue.append(
                        (_type,self.get_doc_id_for_post(doc)))
            else:
                logger.error('update {0}: {1}'.format(_type,path))

    def _process_control_task(self):
        processed = set()
        requeu = []
        while self._postpro_queue:
            _type,_id = self._postpro_queue.pop()
            _hash = hash(frozenset(_id.items()))
            # insert into postprocessing queue
            if not _hash in processed:
                processed.add(_hash)
                self.postprocess(_type,_id)
            else:
                # requeu the unfinished future
                requeu.append((_type,_id))
        for elem in requeu:
            self._postpro_queue.append(elem)

    def postprocess(self,_type,_id):
        # test if this doc exists in post
        colname = '{0}_post'.format(_type)
        self.db[_type].ensure_index('n_min')
        postdoc = self.db[colname].find_one({'_id':_id})
        criteria = {'processed':True}
        criteria.update(_id)
        docs = self.db[_type].find(criteria,sort=[('n_min', 1)])
        if docs.count() > 0:
            # we need to completely reprocess the doc
            if postdoc is None or docs[0]['n_min'] < postdoc['n_min']:
                # create or reset the postdoc
                self.db[colname].update(
                    {'_id':_id},
                    {'_id':_id,'n_min':1e20,'n_max':0},upsert=True)
                docs = list(docs)
                if postdoc is None:
                    postdoc = self.db[colname].find_one({'_id':_id})
            else:
                docs = list(docs.min([('n_min',postdoc['n_max'])]))
            # submit next processing step
            if colname in self.post_processors and len(docs) > 0:
                logger.debug('normal post {0} and queue docs {1}'.format(
                    postdoc['_id']['path'],[(x['n_min'],x['n_max']) for x in docs]))
                future = self.executor.submit(
                        self.post_processors[colname],postdoc,docs)
                future.add_done_callback(self._update_db_and_queu)

    def get_doc_id_for_post(self,doc):
        return {
            'project':doc['project'],
            'path':doc['path'],
            'root_path':doc['root_path'],
            'initial':doc['initial'],
            'host':doc['host'],
        }

    def handle_msg(self,msg):
        # traverse msg
        msg_hash = hash(msg)
        doc = json.loads(msg)
        colname = doc['type']
        doc['hash'] = msg_hash
        doc['modified'] = datetime.datetime.utcnow()
        # insert the basic document
        doc['_id'] = self.db[colname].insert(doc)
        # send response to harvester
        self.server_push_socket.send_multipart(
            ['confirm',json.dumps({'hash':msg_hash})])
        # log that we saved the doc
        logger.info('inserted {0}: {1}'.format(doc['type'],doc['path']))
        # submit the processing
        if colname in self.processors:
            #self.processors[colname](doc)
            future = self.executor.submit(self.processors[colname],doc)
            future.add_done_callback(self._update_db_and_queu)

    def load_conf(self,conffile):
        if conffile is None:
            if 'XDG_CONFIG_HOME' in os.environ:
                confdir = os.path.join(os.environ['XDG_CONFIG_HOME'],'foamserver')
            else:
                confdir = os.path.join(os.environ['HOME'],'.foamserver')
            conffile = os.path.join(confdir,'server.yaml')
            try:
                os.makedirs(confdir)
            except:
                pass
        else:
            confdir = None
        if not os.path.isfile(conffile):
            with open(conffile,'w') as f:
                if confdir is not None:
                    f.write('confdir: {0}\n'.format(confdir))
                    f.write('logfile: {0}\n'.format(os.path.join(confdir,'server.log')))

        with open(conffile,'r') as f:
            self.conf = yaml.load(f.read())
        if 'logfile' in self.conf:
            self.logfile = self.conf['logfile']
        elif 'confdir' in self.conf:
            self.logfile = os.path.join(self.conf['confdir'],'server.log')
        elif confdir:
            self.logfile = os.path.join(confdir,'server.log')
        else:
            self.logfile = os.path.join(os.environ['HOME'],'.foamserver_server.log')

    def handle_response(self,msg):
        if msg[0] == 'new_msg':
            self.handle_msg(msg[1])

    def connect(self):
        self.server_push_socket = self.context.socket(zmq.PUSH)
        self.server_push_socket.bind("tcp://*:{0}".format(self.SERVER_PUSH_PORT))
        self.server_pull_socket = self.context.socket(zmq.PULL)
        self.server_pull_socket.bind("tcp://*:{0}".format(self.SERVER_PULL_PORT))
        self.stream = zmqstream.ZMQStream(self.server_pull_socket, self._loop)
        self.stream.on_recv(self.handle_response)

    def teardown(self):
        self.server_pull_socket.close(linger=0)
        self.server_push_socket.close(linger=0)
        self.client.close()
        self.context.term()

    def start(self):
        self.connect()
        logger.info("started server")
        #self._main_control_loop.start()
        self._loop.start()
        self.executor.shutdown(wait=True)

    def interrupt_handler(self,signum,frame):
        if multiprocessing.current_process().name == 'MainProcess':
            print('catched interrupt, shutting down')
            self._loop.stop()

    def add_logging_file_handler(self):
        fh = logging.FileHandler(self.logfile)
        fh.setLevel(logging.INFO)
        fh.setFormatter(fm)
        logger.addHandler(fh)


@click.command()
@click.option('--debug/--no-debug')
@click.option('--loglevel',default='error')
def main(debug=False,loglevel='error'):
    try:
        server = FoamServer(debug=debug,loglevel=loglevel)
    except KeyboardInterrupt:
        print('catched keyboard interrupt, quitting')
    else:
        signal.signal(signal.SIGINT, server.interrupt_handler)
        server.start()
