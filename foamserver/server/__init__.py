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
import collections
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from zmq.eventloop import ioloop, zmqstream

fmt = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.ERROR,format=fmt)
logger = logging.getLogger('foamserver_server')
logger.setLevel(logging.INFO)
fm = logging.Formatter(fmt)

class DatPostProcessor(object):
    def __call__(self,docs,postdoc):
        ncols = len(docs[0]['data'][-1]['data'])
        data = [[] for i in range(ncols)]
        result = {
            '$max':{'n_max':docs[-1]['n_max']},
            '$min':{'n_min':docs[0]['n_min']},
            '$set':{
                'file':docs[0]['file'],
                'name':docs[0]['name'],
                'starttime':docs[0]['starttime'],
            }
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
            result['$push'] = push = {}
            for i,x in enumerate(head):
                push['data.{0}'.format(x)] = {'$each':data[i]}
        return postdoc,result


class LogPostProcessor(object):
    def __call__(self,docs,postdoc):
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
        result['$push'] = push = {}
        for x in initial:
            push['data.iterations.initial residual.{0}'.format(x)] = {'$each':initial[x]}
        return postdoc,result


class FoamServerException(Exception):
    pass


class LogProcessor(object):
    def __call__(self,doc):
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


class SystemProcessor(object):
    def __call__(self,doc):
        return doc


class DatProcessor(object):
    def __call__(self,doc):
        path_pieces = doc['path'].split('/')
        doc['file'] = path_pieces[-1]
        doc['starttime'] = path_pieces[-2]
        doc['name'] = path_pieces[-3]
        if 'forces' in path_pieces[-1]:
            self.process_forces(doc)
        else:
            self.process_scalar(doc)
        return doc


    def process_forces(self,doc):
        for line in doc['data']:
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


    def process_scalar(self,doc):
        for line in doc['data']:
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
        self.db = self.client['ventilator']
        self._stop = False
        self.context = zmq.Context()
        self._loop = ioloop.IOLoop.instance()
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
        self._results = collections.deque()
        self._main_control_loop = ioloop.PeriodicCallback(
            self._main_control_task,self.SLEEP_TIME*1e3,self._loop)

    def _main_control_task(self):
        for future in as_completed(self._results):
            print(future.result()['path'])

    def get_doc_id_for_post(self,doc):
        return {
            'project':doc['project'],
            'path':doc['path'],
            'root_path':doc['root_path'],
            'initial':doc['initial'],
            'host':doc['host'],
        }

    def post_process_callback(self,fn):
        doc,res = fn.result()
        ok = self.db[doc['type']].update({'_id':doc['_id']},res)
        logger.info(
            'postpro done for: {0}'.format(doc['path']))

    def process_callback(self,fn):
        doc = fn.result()
        # update the processed doc
        self.db[doc['type']].update({'_id':doc['_id']},doc)
        logger.info('updated doc: {0}'.format(doc['path']))
        # test if this doc exists in post
        _id = self.get_doc_id_for_post(doc)
        colname = '{0}_post'.format(doc['type'])
        postdoc = self.db[colname].find_one(_id)
        initial_postdoc = _id.copy()
        initial_postdoc['type'] = colname
        initial_postdoc['n_max'] = 0
        initial_postdoc['n_min'] = 10000
        initial_postdoc['modified'] = datetime.datetime.utcnow()
        # decide following processing steps
        if doc['type'] in ['log','dat']:
            # we need to completely reprocess the doc
            if postdoc is None or doc['n_min'] < postdoc['n_min']:
                self.db[colname].update(_id,initial_postdoc,upsert=True)
                docs = list(self.db[doc['type']].find(_id,sort=[('n_min',1)]))
                postdoc = self.db[colname].find_one(_id)
            else:
                docs = [doc]
        # submit next processing step
        if colname in self.post_processors:
            fn = self.executor.submit(
                self.post_processors[colname],docs,postdoc)
            fn.add_done_callback(self.post_process_callback)

    def handle_msg(self,msg):
        # traverse msg
        msg_hash = hash(msg)
        doc = json.loads(msg)
        doc['type'] = colname = doc['type']
        doc['hash'] = msg_hash
        # insert the basic document
        doc['_id'] = self.db[colname].insert(doc)
        # send response to harvester
        self.server_push_socket.send_multipart(
            ['confirm',json.dumps({'hash':msg_hash})])
        # log that we saved the doc
        logger.debug('inserted for project: {0} path: {1}'.format(
            doc['project'],doc['path']))
        # submit the processing
        if colname in self.processors:
            fn = self.executor.submit(self.processors[colname],doc)
            fn.add_done_callback(self.process_callback)

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
