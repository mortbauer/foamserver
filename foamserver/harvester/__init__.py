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
import click
import signal
import socket
import datetime
import logging
import pprint
import redis
import gevent
import gevent.event
import gevent.select
import gevent.fileobject
import hashlib
from json import dumps, loads
import zmq.green as zmq
import watchdog.observers
from watchdog.observers.api import ObservedWatch
from watchdog.events import RegexMatchingEventHandler

VERSION = 0.2

class HarvesterException(Exception):
    pass

INITIAL_DATA = {
    'version':VERSION,
    'state':{'system':{},'log':{},'dat':{}},
    'harvester_starttime':datetime.datetime.utcnow(),
    'uuid':uuid.uuid4().get_hex(),
}

INITIAL_CONFIG = {
    'watch':{
        'system':[{
            'path':'system',
            'regexes':['.*'],
            'recursive':False,
        }],'log':[{
            'path':'./',
            'regexes':['log\..*'],
            'recursive':False,
        }],'dat':[{
            'path':'postProcessing',
            'regexes':['.*\.dat'],
            'recursive':True,
        }]
    }
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


def convert_from(data,version):
    ndata = copy.deepcopy(INITIAL_DATA)
    if version == 0.0:
        ndata['state']['system'] = data['system']
        ndata['state']['log'] = data['logs']
        ndata['state']['dat'] = data['postProcessing']

    return ndata

def decode_datetime(obj):
    if b'__datetime__' in obj:
        obj = datetime.datetime.strptime(obj["__datetime__"], "%Y%m%dT%H:%M:%S.%f")
    return obj

def encode_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.strftime("%Y%m%dT%H:%M:%S.%f")
    return obj


class EventHandler(RegexMatchingEventHandler):
    def __init__(self,redis,queuename,proctype,regexes=None):
        super(EventHandler,self).__init__(regexes=regexes,ignore_directories=True)
        self.redis = redis
        self.queuename = queuename
        self.type = proctype

    def enqueue(self,path):
        self.redis.rpush(self.queuename,dumps((self.type,path)))

    def init(self,path,recursive):
        if recursive:
            for root, dirs, files in os.walk(path):
                for p in files:
                    if any(r.match(p) for r in self.regexes):
                        self.enqueue(os.path.join(root,p))
        else:
            for p in os.listdir(path):
                if any(r.match(p) for r in self.regexes):
                    self.enqueue(os.path.join(path,p))

    def on_modified(self,event):
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
    TYPE = 'system'

    def test_reset(self,path):
        if path not in self.data:
            return True

    def initialize(self,path):
        self.data[path] = {
            'msg_number':0,
            'initially_tracked':datetime.datetime.utcnow(),
            'hash':None,
        }

    def process(self,path):
        d = self.data[path]
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
    REGEXES = ['.*\.dat']
    TYPE = 'dat'

    def test_reset(self,path):
        if path not in self.data:
            return True
        elif os.stat(path).st_size < self.data[path]['pos']:
            return True

    def initialize(self,path):
        self.data[path] = {
            'msg_number':0,
            'initially_tracked':datetime.datetime.utcnow(),
            'pos':0,
            'line_number':0
        }

    def process(self,path):
        d = self.data[path]
        nd = lambda n_min,n_max:{
            'msg_number':d['msg_number'],
            'path':path,
            'n_min':n_min,
            'n_max':n_max,
            'initially_tracked':d['initially_tracked'],
        }
        with open(path,'r') as f:
            f.seek(d['pos'])
            data = []
            counter = 0
            nlines = d['line_number']
            # since we rely on f.tell() we need to disable the read-ahaead
            # buffer which is normally used due to pewrformance
            # see also: http://stackoverflow.com/q/14145082
            for line in iter(f.readline, ''):
                data.append(line)
                if counter >= self.MAX_LINES:
                    yield nd(d['line_number'],nlines),data
                    d['msg_number'] += 1
                    d['pos'] = f.tell()
                    d['line_number'] = nlines
                    data = []
                    counter = 0
                counter += 1
                nlines += 1
            if len(data) > 0:
                yield nd(d['line_number'],nlines-1),data
                d['msg_number'] += 1
                d['pos'] = f.tell()
                d['line_number'] = nlines

class LogProcessor(DatProcessor):
    REGEXES = ['log\.']
    TYPE = 'log'


class Harvester(object):
    SLEEP_TIME = 2
    PERSISTENCE = '.harvester.json'
    SERVER_PUSH_PORT = 5051
    SERVER_PULL_PORT = 5052
    CONF = 'harvester.yaml'
    MAX_PILE_UP = 200
    RCVTIMEO = 1000
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
        self.meta = {
            'host':socket.gethostname(),
            'root_path':os.path.abspath('.'),
        }
        self.load_conf()
        self.load_state()
        self.context = zmq.Context()
        self.s_sock = self.context.socket(zmq.PUSH)
        self.r_sock = self.context.socket(zmq.PULL)
        self.r_sock.hwm = 100
        self.connect_sockets()
        self._uncon = self.make_redis_name('unconfirmed')
        self._pqueue = self.make_redis_name('processing')
        self._squeue = self.make_redis_name('sending')
        self._store = self.make_redis_name('payloadstore')
        self.uncon_len = lambda : self.redis.scard(self._uncon)
        self.pqueu_len = lambda : self.redis.llen(self._pqueue)
        self.squeu_len = lambda : self.redis.llen(self._squeue)
        self.observer = watchdog.observers.Observer()
        self.processors = {x.TYPE:x(self.state) for x in self.PROCESSORS}
        self.resend_finnished = gevent.event.Event()

    def make_redis_name(self,name):
        return 'foamserver_harvester_{0}_{1}'.format(self.uuid,name)

    def connect_sockets(self):
        self.s_sock.connect('tcp://{0}:{1}'.format(
            self.conf.get('host','localhost'),
            self.conf.get('port',self.SERVER_PULL_PORT)))

        self.r_sock.connect('tcp://{0}:{1}'.format(
            self.conf.get('host','localhost'),
            self.conf.get('pull_port',self.SERVER_PUSH_PORT)))

    def load_conf(self):
        if not os.path.isfile(self.CONF):
            raise click.ClickException(
                'failed to load {0} which is required.'.format(self.CONF))
        with open(self.CONF,'r') as f:
            self.conf = yaml.load(f.read())
        try:
            self.meta['project'] = self.conf['project']
        except KeyError:
            raise click.ClickException('you need to specify a project name in the configuration')
        try:
            self.watch = self.conf['watch']
            assert type(self.watch) == dict
        except KeyError:
            raise click.ClickException('you need to specify a project name in the configuration')

    def load_state(self):
        self.data = copy.deepcopy(INITIAL_DATA)
        try:
            with open(self.PERSISTENCE,'r') as f:
                data = json.loads(f.read())
                stored_version = data.get('version',0.0)
                if stored_version < 0.2:
                    data = convert_from(data,stored_version)
                if not 'uuid' in data:
                    logger.critical(
                        'probably corrupted: {0} no uuid'.format(self.PERSISTENCE))
                self.data.update(data)
            logger.info('loaded state from: {0}'.format(self.PERSISTENCE))
        except:
            logger.error('failed to load {0}'.format(self.PERSISTENCE))
        self.state = self.data['state']
        self.uuid = self.data['uuid']
        #self.meta['harvester_starttime'] = self.data['harvester_starttime']
        self.meta['uuid'] = self.uuid
        #self.meta['harvester_version'] = VERSION

    def save_state(self):
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
        for _type in self.watch:
            if _type not in self.processors:
                logger.error('no handler of type: {0}'.format(_type))
                continue
            processor = self.processors[_type]
            for item in self.watch[_type]:
                path = item['path']
                recursive = item.get('recursive')
                handler = EventHandler(
                    self.redis,self._pqueue,_type,
                    regexes=item.get('regexes',processor.REGEXES))
                # fire my initialitation
                handler.init(path,recursive)
                self.observer.schedule(handler,path,recursive=recursive)

    def resend(self):
        i = 0
        for key_msg in self.redis.hkeys(self._store):
            self.redis.rpush(self._squeue,key_msg)
            i += 1
        if i:
            return True

    def receive_loop(self):
        logger.debug('receive_loop ready')
        while not self._shutdown:
            # break if we wanna stop and have everything confirmed
            if self._processing_done and not self.uncon_len():
                break
            try:
                msg = self.r_sock.recv_multipart(flags=zmq.NOBLOCK)
            except zmq.ZMQError:
                gevent.sleep(1.)
            else:
                if 'confirm' == msg[0]:
                    payload = self.redis.hget(self._store,msg[1])
                    if payload:
                        if hash(payload) == loads(msg[2]):
                            ok = self.redis.srem(self._uncon,msg[1])
                            ok = self.redis.hdel(self._store,msg[1])
                    else:
                        logger.error('not in store: %s',msg[1])
        logger.debug('ending receive_loop')

    def send_loop(self):
        logger.debug('send_loop ready')
        while not self._stop:
            key_msg = self.redis.lpop(self._squeue)
            if key_msg:
                if logger.level <= logging.INFO:
                    key = loads(key_msg)
                    logger.info('sending: %s::%s',key['doc']['path'],key['doc']['msg_number'])
                payload_msg = self.redis.hget(self._store,key_msg)
                # it could be that the item was removed because it was confirmed
                if payload_msg:
                    self.redis.sadd(self._uncon,key_msg)
                    self.s_sock.send_multipart(['new',key_msg,payload_msg])
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
            task = self.redis.lpop(self._pqueue)
            if task:
                _type, path = loads(task)
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
                logger.degug('process loop sleeping, nothing to do')
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
            self.s_sock.close(linger=0)
            self.r_sock.close(linger=0)
            self.context.term()

    def stop(self):
        self._stop = True
        self.stop_observer()
        if self.redis.scard(self._uncon) > 0:
            return False
        else:
            return True

    def start(self,oneshot=False):
        self.redis = redis.StrictRedis()
        try:
            self.init_handlers()
            # reque the unconfirmed messages, but give some time to get confirmation
            # before
            wait = 0.0
            if self.resend():
                wait = 2.0
            # starte or not start observer
            if not oneshot:
                self.start_observer()
            else:
                self._oneshot = True
            # set our signal handler and save reference to default
            self.default_signal_handler = signal.signal(
                signal.SIGINT, self.loop_interrupt_handler)
            # now block until finnished
            gevent.joinall([
                gevent.spawn(self.process_loop),
                gevent.spawn(self.send_loop),
                gevent.spawn(self.receive_loop),
            ])
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
@click.option('--oneshot',default=False,is_flag=True)
@click.option('-d','--debug',default=False,is_flag=True)
@click.option('-l','--loglevel',default='error')
def start(oneshot,debug,loglevel):
    if hasattr(logging,loglevel.upper()):
        logger.setLevel(loglevel.upper())
    else:
        logger.error('there is no "{0}" loglevel'.format(loglevel))
    if debug:
        logger.setLevel(logging.DEBUG)
    harvester = Harvester()
    harvester.start(oneshot=oneshot)


@run.command()
def info():
    harvester = Harvester()
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
@click.option('--force',is_flag=True)
@click.option('--project')
def init(project,force):
    if not os.path.exists(Harvester.CONF) or force or \
            click.confirm('harvester.yaml exists, really overwrite?'):
        if project is None:
            project = str(click.prompt('project name'))
        config = copy.deepcopy(INITIAL_CONFIG)
        config['project'] = project
        with open(Harvester.CONF,'w') as f:
            yaml.dump(config,f)

