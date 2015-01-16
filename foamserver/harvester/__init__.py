#coding=utf-8

import os
import re
import sys
import zmq
import stat
import json
import yaml
import time
import copy
import click
import signal
import socket
import datetime
import logging
from watchdog.observers import Observer
from watchdog.observers.api import ObservedWatch
from .utils import SystemEventHandler, DatEventHandler, LogEventHandler

VERSION = 0.2

INITIAL_DATA = {
    'cache':{'system':{},'log':{},'dat':{}},
    'harvester_starttime':datetime.datetime.utcnow(),
    'msgs':[],
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

fmt = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.ERROR,format=fmt)
logger = logging.getLogger('harvester')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('.harvester.log')
fh.setLevel(logging.INFO)
fm = logging.Formatter(fmt)
fh.setFormatter(fm)
logger.addHandler(fh)


def convert_from(data,version):
    ndata = copy.deepcopy(INITIAL_DATA)
    if version == 0.0:
        ndata['cache']['system'] = data['system']
        ndata['cache']['log'] = data['logs']
        ndata['cache']['dat'] = data['postProcessing']
        ndata['msgs'] = data['msgs']

    return ndata


def custom_schedule(self, event_handler, path, recursive=False):
    """
    Schedules watching a path and calls appropriate methods specified
    in the given event handler in response to file system events.
    :param event_handler:
    An event handler instance that has appropriate event handling
    methods which will be called by the observer in response to
    file system events.
    :type event_handler:
    :class:`watchdog.events.FileSystemEventHandler` or a subclass
    :param path:
    Directory path that will be monitored.
    :type path:
    ``str``
    :param recursive:
    ``True`` if events will be emitted for sub-directories
    traversed recursively; ``False`` otherwise.
    :type recursive:
    ``bool``
    :return:
    An :class:`ObservedWatch` object instance representing
    a watch.
    """
    with self._lock:
        watch = ObservedWatch(path, recursive)
        self._add_handler_for_watch(event_handler, watch)
        # If we don't have an emitter for this watch already, create it.
        if self._emitter_for_watch.get(watch) is None:
            emitter = self._emitter_class(event_queue=self.event_queue,
            watch=watch,
            timeout=self.timeout)
            self._add_emitter(emitter)
            if self.is_alive():
                emitter.start()
                self._watches.add(watch)
        # call init
        event_handler.init(path,recursive)
    return watch

Observer.schedule = custom_schedule

class DictEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)

class Harvester(object):
    REQUEST_TIMEOUT = 5500
    SLEEP_TIME = 2
    PERSISTENCE = '.harvester.json'
    CONF = 'harvester.yaml'

    def __init__(self,data=None,logging=True):
        self._socket = None
        self._stop = False
        self._started = False
        self._oneshot = False
        self.load_conf()
        self.context = zmq.Context()
        self.hostname = socket.gethostname()
        self.server_addr = 'tcp://{0}:{1}'.format(
            self.conf.get('host','localhost'),
            self.conf.get('port',5051))
        self.load_state(data)

    def load_conf(self):
        if not os.path.isfile(self.CONF):
            raise Exception(
                'failed to load {0} which is required.'.format(self.CONF))
        with open(self.CONF,'r') as f:
            self.conf = yaml.load(f.read())
        try:
            self.project = self.conf['project']
        except KeyError:
            raise Exception('you need to specify a project name in the configuration')
        try:
            self.watch = self.conf['watch']
            assert type(self.watch) == dict
        except KeyError:
            raise Exception('you need to specify a project name in the configuration')

    def load_state(self,data):
        self.data = copy.deepcopy(INITIAL_DATA)
        if data is not None:
            self.data.update(data)
        #try:
        with open(self.PERSISTENCE,'r') as f:
            data = json.loads(f.read())
            stored_version = data.get('version',0.0)
            if stored_version < 0.2:
                data = convert_from(data,stored_version)
            self.data.update(data)
        #except:
            #logger.error('failed to load {0}'.format(self.PERSISTENCE))
        self.msgs = self.data['msgs']
        self.cache = self.data['cache']
        self.data['version'] = VERSION

    def create_observer(self):
        self.observer = observer = Observer()
        for handler_type in self.watch:
            if handler_type == 'system':
                Handler = SystemEventHandler
            elif handler_type == 'log':
                Handler = LogEventHandler
            elif handler_type == 'dat':
                Handler = DatEventHandler
            for item in self.watch[handler_type]:
                observer.schedule(
                    Handler(self.msgs,self.cache,regexes=item['regexes']),
                    item['path'],recursive=item.get('recursive'))

    @property
    def socket(self):
        if self._socket is None:
            self._socket = self.context.socket(zmq.REQ)
            self._socket.connect(self.server_addr)
            self._socket.RCVTIMEO = self.REQUEST_TIMEOUT
        return self._socket

    def close_socket(self):
        if self._socket is not None:
            self._socket.setsockopt(zmq.LINGER, 0)
            self._socket.close()
            self._socket = None

    def send_msg(self,n):
        msg = json.dumps({
            'project':self.project,
            'host':self.hostname,
            'harvester_starttime':self.data['harvester_starttime'],
            'data':self.msgs[:n],
        },cls=DictEncoder)
        try:
            self.socket.send(msg)
            res = json.loads(self.socket.recv())
            if res['state'] == 'Ok':
                del self.msgs[:n]
        except zmq.error.Again:
            logger.error('zmq error, try reconnect')
            self.close_socket()
        except zmq.error.ZMQError as e:
            if self._stop:
                logger.debug('ignore zmq.error.ZMQError _stop == True')
            else:
                raise zmq.error.ZMQError(e)

    def start_send_recv_loop(self):
        n = len(self.msgs)
        while not self._stop and not (self._oneshot and n == 0):
            if n > 0:
                self.send_msg(n)
            else:
                time.sleep(self.SLEEP_TIME)
            n = len(self.msgs)
        self.stop()

    def observe(self):
        if not self._started:
            self.create_observer()
            self.observer.start()
            self._started = True
            self._stop = False
            self.start_send_recv_loop()

    def stop(self):
        if not self._stop:
            self._stop = True
            self._started = False
            self.teardown()

    def save_state(self):
        try:
            if os.path.isfile(self.PERSISTENCE):
                os.chmod(self.PERSISTENCE,stat.S_IWRITE|stat.S_IRGRP|stat.S_IROTH)
            with open(self.PERSISTENCE,'w') as f:
                f.write(json.dumps(self.data,indent=2,cls=DictEncoder))
            # mark as read only
            os.chmod(self.PERSISTENCE,stat.S_IREAD|stat.S_IRGRP|stat.S_IROTH)
        except KeyboardInterrupt:
            logger.critical('interrupted the save state')

    def teardown(self):
        if self._started:
            self.observer.stop()
        self.save_state()
        self.close_socket()
        self.context.term()
        if self._started:
            self.observer.join()

    def oneshot(self):
        if not self._started:
            self.create_observer()
            self._started = True
            self._oneshot = True
            self._stop = False
            self.start_send_recv_loop()


@click.group()
def run():
    pass

@run.command()
def observe():
    try:
        harvester = Harvester()
    except KeyboardInterrupt:
        print('catched keyboard interrupt on initalization')
    else:
        def interupt_handler(signum,frame):
            print('catched interrupt, shutting down')
            harvester.stop()
        signal.signal(signal.SIGINT, interupt_handler)
        harvester.observe()

@run.command()
def oneshot():
    try:
        harvester = Harvester()
    except KeyboardInterrupt:
        print('catched keyboard interrupt on initalization')
    else:
        def interupt_handler(signum,frame):
            print('catched interrupt, shutting down')
            harvester.stop()
        signal.signal(signal.SIGINT, interupt_handler)
        harvester.oneshot()

@run.command()
def info():
    try:
        harvester = Harvester()
    except KeyboardInterrupt:
        print('catched keyboard interrupt on initalization')
    else:
        for key in harvester.cache:
            print('{0}:'.format(key))
            for path in harvester.cache[key]:
                print('    {0}:'.format(path))

@run.command()
@click.argument('pattern')
def reset_cache(pattern):
    try:
        harvester = Harvester()
    except KeyboardInterrupt:
        print('catched keyboard interrupt on initalization')
    else:
        PATTERN = re.compile(pattern)
        data = {}
        for key in harvester.cache:
            data[key]=[]
            for path in harvester.cache[key]:
                if PATTERN.match(path):
                    data[key].append(path)
        # ask
        print('paths marked for resetting:\n')
        any_matched = False
        for key in data:
            if len(data[key]):
                print('{0}:'.format(key))
            for path in data[key]:
                any_matched = True
                print('    {0}:'.format(path))
        if any_matched:
            if click.confirm('\nreally clear cache for marked paths ?'):
                for key in data:
                    for path in data[key]:
                        del harvester.cache[key][path]
            harvester.save_state()
        else:
            print('nothing matched.')

@run.command()
def init():
    if not os.path.exists(Harvester.CONF) or \
            click.confirm('file harvester.yaml already exists, should I really overwrite it?'):
        project = str(click.prompt('project name'))
        config = copy.deepcopy(INITIAL_CONFIG)
        config['project'] = project
        with open(Harvester.CONF,'w') as f:
            yaml.dump(config,f)

