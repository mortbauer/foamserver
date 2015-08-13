#coding=utf-8

import os
import re
import abc
import sys
import stat
import uuid
import json
import yaml
import time
import copy
import Queue
import click
import signal
import socket
import datetime
import logging
import pprint
import cerberus
import redis
import gevent
import gevent.select
import hashlib
from json import dumps, loads
import zmq.green as zmq
import watchdog.observers
from watchdog.observers.api import ObservedWatch
from watchdog.events import RegexMatchingEventHandler

VERSION = 0.3

CONFIG_SCHEMA = {
    'project': {'type': 'string','required':True},
    'redis_host': {'type': 'string','required':False},
    'redis_port': {'type': 'string','required':False},
    'redis_password': {'type': 'string','required':False},
    'server_host': {'type': 'string','required':False},
    'server_post': {'type': 'string','required':False},
    'watch':{'type':'list','required':True,'schema':{
        'type':'dict','schema':{
            'type':{
                'type':'string',
                'required':True,
                'allowed':['log','dat','system'],
            },'path':{'type':'string','required':True},
            'regexes':{'type':'list','schema':{'type':'string'}},
            'ignore_regexes':{'type':'list','schema':{'type':'string'}},
            'root_regexes':{'type':'list','schema':{'type':'string'}},
            'recursive':{'type':'boolean'},
        }
    }}
}


#fmt = '%(message)s'
fmt = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.ERROR,format=fmt)
logger = logging.getLogger('harvester')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('.harvester.log')
fh.setLevel(logging.DEBUG)
fm = logging.Formatter(fmt)
fh.setFormatter(fm)
logger.addHandler(fh)


def encode_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.strftime("%Y%m%dT%H:%M:%S.%f")
    return obj


class EventHandler(RegexMatchingEventHandler):
    def __init__(self,redis,queue,proctype,regexes=None,ignore_regexes=None,root_regexes=None):
        super(EventHandler,self).__init__(
            regexes=regexes,ignore_regexes=ignore_regexes,ignore_directories=True)
        self.redis = redis
        self.queue = queue
        self.root_regexes = [] if root_regexes is None else [re.compile(x) for x in root_regexes]
        self.type = proctype

    def enqueue(self,path):
        self.queue.put(dumps((self.type,path)))

    def init(self,path,recursive,last_run_time):
        if recursive:
            for root, dirs, files in os.walk(path):
                if any(r.match(root) for r in self.root_regexes):
                    for p in files:
                        fullpath = os.path.join(root,p)
                        if any(r.match(p) for r in self.regexes) and not \
                                any(r.match(p) for r in self.ignore_regexes):
                            self.enqueue(fullpath)
        else:
            for p in os.listdir(path):
                fullpath = os.path.join(path,p)
                if any(r.match(p) for r in self.regexes) and not \
                        any(r.match(p) for r in self.ignore_regexes):
                    self.enqueue(fullpath)

    def on_modified(self,event):
        logger.debug('eventhandler got change for %s',event.src_path)
        self.enqueue(event.src_path)


class BaseProcessor(object):
    __metaclass__ = abc.ABCMeta
    MAX_LINES = 50000

    def __init__(self,state):
        self.data = state[self.TYPE]

    def __call__(self,path):
        if self.test_reset(path):
            self.initialize(path)
        for meta,payload in self.process(path):
            meta['type'] = self.TYPE
            yield meta,payload

    @abc.abstractmethod
    def process(self,path):
        return

    @abc.abstractmethod
    def initialize(self,path):
        return

    @abc.abstractmethod
    def test_reset(self,path):
        return

class SystemProcessor(BaseProcessor):
    REGEXES = ['[a-z_\-A-Z]+']
    IGNORE_REGEXES = []
    TYPE = 'system'

    def test_reset(self,path):
        if path not in self.data:
            return True

    def initialize(self,path):
        self.data[path] = {
            'msg_number':0,
            'initially_tracked':datetime.datetime.utcnow(),
            'hash':None,
            'mtime':0,
        }

    def process(self,path):
        d = self.data[path]
        if os.stat(path).st_mtime > d['mtime']:
            doc = {
                'msg_number':d['msg_number'],
                'timestamp':time.time(),
                'path':path,
                'initially_tracked':d['initially_tracked'],
            }
            hasher = hashlib.md5()
            with open(path,'r') as f:
                text = f.read()
                hasher.update(text)
                _hash = hasher.hexdigest()

            # send only if data really changed
            if _hash != d['hash']:
                yield doc,text
                d['msg_number'] += 1
                d['hash'] = _hash

class DatProcessor(BaseProcessor):
    REGEXES = ['.*']
    IGNORE_REGEXES = ['.*\.vtk']
    TYPE = 'dat'

    def test_reset(self,path):
        self.hasher = hashlib.md5()
        if path not in self.data:
            return True
        elif os.stat(path).st_size < self.data[path]['pos']:
            return True
        elif os.stat(path).st_mtime > self.data[path]['mtime']:
            with open(path,'r') as f:
                txt = f.read(self.data[path]['pos'])
                self.hasher.update(txt)
                _hash = self.hasher.hexdigest()
            if _hash != self.data[path]['hash']:
                return True


    def initialize(self,path):
        self.hasher = hashlib.md5()
        self.data[path] = {
            'msg_number':0,
            'initially_tracked':datetime.datetime.utcnow(),
            'pos':0,
            'line_number':0,
            'mtime':0,
            'hash':None,
        }

    def process(self,path):
        d = self.data[path]
        nd = lambda n_min,n_max:{
            'msg_number':d['msg_number'],
            'timestamp':time.time(),
            'path':path,
            'n_min':n_min,
            'n_max':n_max,
            'initially_tracked':d['initially_tracked'],
        }
        with open(path,'r') as f:
            d['mtime'] = time.time()
            f.seek(d['pos'])
            data = []
            counter = 0
            nlines = d['line_number']
            # since we rely on f.tell() we need to disable the read-ahaead
            # buffer which is normally used due to pewrformance
            # see also: http://stackoverflow.com/q/14145082
            for line in iter(f.readline, ''):
                data.append(line)
                self.hasher.update(line)
                if counter >= self.MAX_LINES:
                    yield nd(d['line_number'],nlines),data
                    d['msg_number'] += 1
                    d['pos'] = f.tell()
                    d['line_number'] = nlines
                    d['hash'] = self.hasher.hexdigest()
                    data = []
                    counter = 0
                counter += 1
                nlines += 1
            if len(data) > 0:
                yield nd(d['line_number'],nlines-1),data
                d['msg_number'] += 1
                d['pos'] = f.tell()
                d['line_number'] = nlines
                d['hash'] = self.hasher.hexdigest()

class LogProcessor(DatProcessor):
    REGEXES = ['log\.']
    IGNORE_REGEXES = []
    TYPE = 'log'


INITIAL_CONFIG = {
    'watch':[
        {'type':'system',
         'path':'system',
         'recursive':False,
         'ignore_regexes':SystemProcessor.IGNORE_REGEXES,
         'regexes':SystemProcessor.REGEXES},
        {'type':'log',
         'path':'.',
         'recursive':False,
         'ignore_regexes':LogProcessor.IGNORE_REGEXES,
         'regexes':LogProcessor.REGEXES},
        {'type':'dat',
         'path':'postProcessing',
         'recursive':True,
         'ignore_regexes':DatProcessor.IGNORE_REGEXES,
         'regexes':DatProcessor.REGEXES},
    ]
}

processors = {
    'log':LogProcessor,
    'system':SystemProcessor,
    'dat':DatProcessor,
}

class Harvester(object):
    PERSISTENCE = '.harvester.json'
    CONF = 'harvester.yaml'
    SERVER_PORT = 5051
    HWM = 100
    TIMEOUT = 1000
    PROCESSORS = (LogProcessor,DatProcessor,SystemProcessor)

    def __init__(self,logging=True):
        self.redis = None
        self._interrupted = False
        self._shutdown = False
        self._stop = False
        self._stopped = False
        self._observer_started = False
        self._oneshot = False
        self._processing_done = False
        self.timeout = 1
        self.meta = {
            'host':socket.gethostname(),
            'root_path':os.path.abspath('.'),
        }
        self.load_conf()
        self.load_state()
        self.connect()
        self._pqueue = Queue.Queue()
        #self._pqueue = self.make_redis_name('processing')
        self._squeue = self.make_redis_name('sending')
        self._store = self.make_redis_name('payloadstore')
        #self.pqueu_len = lambda : self.redis.llen(self._pqueue)
        self.squeu_len = lambda : self.redis.llen(self._squeue)
        self.observer = watchdog.observers.Observer()
        self.processors = {x.TYPE:x(self.state) for x in self.PROCESSORS}

    def make_redis_name(self,name):
        return 'foamserver_harvester_{0}_{1}'.format(self.uuid,name)

    def connect(self):
        self.context = zmq.Context()
        self.sock = self.context.socket(zmq.DEALER)
        self.sock.set_hwm(self.HWM)
        self.sock.connect('tcp://{0}:{1}'.format(
            self.conf.get('server_host','localhost'),
            self.conf.get('server_port',self.SERVER_PORT)))

    def terminate_connection(self):
        self.sock.close(linger=0)
        self.context.destroy()

    def load_conf(self):
        v = cerberus.Validator(CONFIG_SCHEMA)
        try:
            with open(self.CONF,'r') as f:
                self.conf = yaml.load(f.read())
        except:
            raise click.ClickException(
                'failed to load {0} which is required.'.format(self.CONF))
        if not v.validate(self.conf):
            raise click.ClickException('conf validation failed with:\n %s'%v.errors)

    def load_state(self):
        self.data =  {
            'version':VERSION,
            'state':{'system':{},'log':{},'dat':{}},
            'harvester_initial_starttime':datetime.datetime.utcnow(),
            'uuid':uuid.uuid4().get_hex(),
        }
        try:
            with open(self.PERSISTENCE,'r') as f:
                data = json.loads(f.read())
                stored_version = data.get('version',0.0)
                if stored_version < 0.2:
                    raise ClickException('can\'t continue, old version detected')
                if not 'uuid' in data:
                    logger.critical(
                        'probably corrupted: {0} no uuid'.format(self.PERSISTENCE))
                self.data.update(data)
            logger.info('loaded state from: {0}'.format(self.PERSISTENCE))
        except:
            logger.error('failed to load {0}'.format(self.PERSISTENCE))
        self.state = self.data['state']
        self.last_run_time = self.data.get('save_time',0)
        self.uuid = self.data['uuid']
        #self.meta['harvester_starttime'] = self.data['harvester_starttime']
        self.meta['uuid'] = self.uuid
        self.meta['project'] = self.conf['project']
        self.meta['harvester_starttime'] = datetime.datetime.utcnow()
        self.meta['harvester_version'] = VERSION

    def save_state(self):
        self.data['save_time'] = time.time()
        try:
            if os.path.isfile(self.PERSISTENCE):
                os.chmod(self.PERSISTENCE,stat.S_IWRITE|stat.S_IRGRP|stat.S_IROTH)
            with open(self.PERSISTENCE,'w') as f:
                f.write(json.dumps(self.data,indent=2,default=encode_datetime))
            # mark as read only
            os.chmod(self.PERSISTENCE,stat.S_IREAD|stat.S_IRGRP|stat.S_IROTH)
            logger.debug('saved state to: {0}'.format(self.PERSISTENCE))
        except Exception:
            logger.critical('interrupted the save state')

    def init_handlers(self):
        for item in self.conf['watch']:
            if not os.path.isdir(item['path']):
                logger.error('skipping rule for path: %s, not dir',item['path'])
                continue
            if item['type'] not in self.processors:
                logger.error('no handler of type: {0}'.format(item['type']))
                continue
            processor = self.processors[item['type']]
            recursive = item.get('recursive')
            handler = EventHandler(
                self.redis,self._pqueue,item['type'],
                regexes=item.get('regexes',processor.REGEXES),
                root_regexes=item.get('root_regexes',['.*']),
                ignore_regexes=item.get('ignore_regexes',processor.IGNORE_REGEXES),
            )
            # fire my initialitation
            handler.init(item['path'],recursive,self.last_run_time)
            logger.debug('sheduled observer for path %s %s %s',item['path'],recursive,item.get('regexes',processor.REGEXES))
            self.observer.schedule(handler,item['path'],recursive=recursive)

    def send_receive(self,key_msg,payload_msg):
        self.sock.send_multipart(['','data!',key_msg,payload_msg])
        hashed = str(hash(payload_msg))
        sent = time.time()
        self.timeout = 1
        lastlive = 0
        while not (self._shutdown or self._stop):
            if self.sock.poll(self.TIMEOUT,flags=zmq.POLLIN):
                msg = self.sock.recv_multipart()
                if msg[1] == 'confirm!':
                    if msg[2] == key_msg and msg[3] == hashed:
                        return True
                    else:
                        print('message {0} doesn\'t need confirmmation'.format(msg[2]))
                elif msg[1] == 'alive!':
                    lastlive = time.time()
                    if (float(msg[2])-sent) > 1:
                        try:
                            self.sock.send_multipart(['','data!',key_msg,payload_msg])
                        except zmq.Again as e:
                            logger.error('send_multipart failed "%s"',e)
                        sent = time.time()
                        print('resending %s'%key_msg)
                else:
                    print('unknown command "%s"'%msg[1])
            else:
                if (time.time() - lastlive) > (self.timeout):
                    try:
                        self.sock.send_multipart(['','alive?',str(time.time())],flags=zmq.NOBLOCK)
                    except zmq.Again as e:
                        logger.error('send_multipart failed "%s"',e)
                    self.timeout += 1

    def send_loop(self):
        logger.debug('send_loop ready')
        while not self._stop:
            key_msg = self.redis.lindex(self._squeue,0)
            if key_msg:
                if logger.level <= logging.INFO:
                    key = loads(key_msg)
                    logger.info('sending: %s::%s',key['doc']['path'],key['doc']['msg_number'])
                payload_msg = self.redis.hget(self._store,key_msg)
                # it could be that the item was removed because it was confirmed
                if payload_msg:
                    if self.send_receive(key_msg,payload_msg):
                        self.redis.lpop(self._squeue)
            else:
                # seems we are done
                if self._processing_done:
                    break
                else:
                    gevent.sleep(1.)
        logger.debug('sending done')

    def process_loop(self,*args):
        logger.debug('process_loop ready')
        while not self._stop:
            if not self._pqueue.empty():
                task = self._pqueue.get_nowait()#self.redis.lpop(self._pqueue)
                _type, path = loads(task)
                if os.path.isdir(path):
                    continue
                logger.debug('working on task %s',path)
                relpath = os.path.relpath(path,self.meta['root_path'])
                for meta,payload in self.processors[_type](relpath):
                    key = {'project':self.meta,'doc':meta}
                    key_msg = dumps(key,default=encode_datetime)
                    self.redis.hset(self._store,key_msg,dumps(payload))
                    self.redis.rpush(self._squeue,key_msg)
                    if self._stop:
                        break
            # we are finnished, so break out
            elif self._oneshot:
                break
            # lets sleep a while
            else:
                gevent.sleep(1.)
        # so it seems we are done
        self._processing_done = True
        logger.debug('processing done')

    def start_observer(self):
        if not self._observer_started:
            self.observer.start()
            self._observer_started = True
            logger.info('started observer')

    def stop_observer(self):
        if self._observer_started:
            self.observer.stop()
            self._observer_started = False
            logger.info('stopped observer')

    def kill(self):
        self._shutdown = True
        if not self._stopped:
            self._stopped = True
            self.sock.close(linger=0)
            self.context.term()

    def stop(self):
        self._stop = True
        self.stop_observer()

    def start(self,oneshot=False):
        redis_kwargs = {}
        for x in self.conf:
            if x.startswith('redis_'):
                redis_kwargs[x[6:]] = self.conf[x]
        self.redis = redis.StrictRedis(**redis_kwargs)
        try:
            self.init_handlers()
            # starte or not start observer
            if not oneshot:
                self.start_observer()
            else:
                self._oneshot = True
            # set our signal handler and save reference to default
            self.default_signal_handler = signal.signal(
                signal.SIGINT, self.loop_interrupt_handler)
            # now block until finnished
            loops = [
                gevent.spawn(self.process_loop),
                gevent.spawn(self.send_loop),
            ]
            gevent.joinall(loops)
            # save the state,
            signal.signal(signal.SIGINT, self.save_interrupt_handler)
            self.save_state()
        except redis.ConnectionError:
            raise click.ClickException(
                'connection to redis with:\n\n {0}\n\n failed!'.format(
                    pprint.pformat(self.redis.connection_pool.connection_kwargs)))
        #self.redis.save()
        #logger.debug('saved redis data')

    def input_loop(self):
        while not self._shutdown:
            i,o,e = gevent.select.select([sys.stdin],[],[],0.0001)
            if i:
                value = sys.stdin.readline().strip()
                if value in ('y','yes'):
                    self.kill()
                else:
                    print('continue waiting!')
                signal.signal(signal.SIGINT, self.loop_interrupt_handler)
            elif (time.time()-self._interrupt_time) > 5:
                # reshedule normal interrupt_handler
                signal.signal(signal.SIGINT, self.loop_interrupt_handler)

    def kill_handler(self,signum,frame):
        self.kill()

    def loop_interrupt_handler(self,signum,frame):
        if not self.stop():
            self._interrupt_time = time.time()
            print('there are unconfirmed msgs, really quit [y/N]:')
            signal.signal(signal.SIGINT,self.kill_handler)
            gevent.spawn(self.input_loop)
        else:
            print('almost done, be patient')

    def save_interrupt_handler(self,signum,frame):
        signal.signal(signal.SIGINT, self.default_signal_handler)
        if click.confirm('saving state, really quit now? [y/N]:'):
            logger.warn('focing shutdown')
            raise KeyboardInterrupt




@click.group()
def run():
    pass

@run.command()
@click.option('--pattern')
@click.option('--oneshot',default=False,is_flag=True)
@click.option('-d','--debug',default=False,is_flag=True)
@click.option('-l','--loglevel',default='error')
def start(oneshot,debug,loglevel,pattern):
    if hasattr(logging,loglevel.upper()):
        logger.setLevel(loglevel.upper())
    else:
        logger.error('there is no "{0}" loglevel'.format(loglevel))
    if debug:
        logger.setLevel(logging.DEBUG)
    harvester = Harvester()
    if pattern is not None:
        PATTERN = re.compile(pattern)
        newdict = []
        for item in harvester.conf['watch']:
            if PATTERN.match(item['path']):
                newdict.append(item)
        harvester.conf['watch'] = newdict
    harvester.start(oneshot=oneshot)


@run.command('uuid')
def print_uuid():
    logger.setLevel('CRITICAL')
    harvester = Harvester()
    print(str(harvester.uuid))

@run.command()
def info():
    logger.setLevel('CRITICAL')
    harvester = Harvester()
    print('project uuid: {0}'.format(harvester.uuid))
    print('tracked files are:\n')
    for key in harvester.state:
        print('{0}:'.format(key))
        for path in harvester.state[key]:
            print('    {0}:'.format(path))

@run.command()
@click.argument('pattern')
@click.option('--force',is_flag=True)
@click.option('--quiet',is_flag=True)
@click.option('-d','--debug',default=False,is_flag=True)
@click.option('-l','--loglevel',default='error')
def reset_cache(pattern,force,debug,loglevel,quiet):
    if hasattr(logging,loglevel.upper()):
        logger.setLevel(loglevel.upper())
    else:
        logger.error('there is no "{0}" loglevel'.format(loglevel))
    if debug:
        logger.setLevel(logging.DEBUG)
    harvester = Harvester()
    PATTERN = re.compile(pattern)
    data = {}
    for key in harvester.state:
        data[key]=[]
        for path in harvester.state[key]:
            if PATTERN.match(path):
                data[key].append(path)
    # ask
    if not force:
        print('paths marked for resetting:\n')
    any_matched = False
    for key in data:
        if not force and len(data[key]):
            print('{0}:'.format(key))
        for path in data[key]:
            any_matched = True
            if not force:
                print('    {0}:'.format(path))
    if any_matched:
        if force or click.confirm('\nreally clear state for marked paths ?'):
            for key in data:
                for path in data[key]:
                    if not quiet:
                        print('resetting state for: %s %s'%(key,path))
                    del harvester.state[key][path]
        harvester.save_state()
    else:
        print('nothing matched.')

@run.command()
@click.option('-p','--project')
@click.option('--force',is_flag=True)
@click.option('-d','--directory',default='.')
def init(project,force,directory):
    filepath = os.path.join(directory,Harvester.CONF)
    if not os.path.exists(filepath) or force or \
            click.confirm('harvester.yaml exists, really overwrite?'):
        if project is None:
            project = str(click.prompt('project name'))
        config = copy.deepcopy(INITIAL_CONFIG)
        config['project'] = project
        with open(filepath,'w') as f:
            yaml.dump(config,f)

@run.command()
@click.option('-d','--directory',default='.')
@click.argument('path',nargs=-1,required=True)
@click.option('--type',nargs=1,required=True,type=click.Choice(['log','dat','system']))
@click.option('--regexes')
@click.option('--recursive',is_flag=True)
def add_to_watch(path,type,directory,regexes,recursive):
    filepath = os.path.join(directory,Harvester.CONF)
    with open(filepath,'r') as f:
        d = yaml.load(f)
    if d['watch'] is None:
        d['watch'] = []
    for p in path:
        d['watch'].append({
            'type':type,
            'regexes':regexes if regexes else processors[type].REGEXES,
            'recursive':recursive,
            'path':p
        })
    with open(filepath,'w') as f:
        yaml.dump(d,f)



@run.command()
@click.option('-d','--directory',default='.')
def convert_config(directory):
    filepath = os.path.join(directory,Harvester.CONF)
    with open(filepath,'r') as f:
        d = yaml.load(f)
    watch = []
    for _type in d['watch']:
        for item in d['watch'][_type]:
            item['type'] = _type
            watch.append(item)
    d['watch'] = watch
    with open(filepath,'w') as f:
        yaml.dump(d,f)

@run.command()
@click.option('-d','--directory',default='.')
def validate_config(directory):
    filepath = os.path.join(directory,Harvester.CONF)
    with open(filepath,'r') as f:
        d = yaml.load(f)
    v = cerberus.Validator(CONFIG_SCHEMA)
    if not v.validate(d):
        print('validation failed with:\n %s'%v.errors)






