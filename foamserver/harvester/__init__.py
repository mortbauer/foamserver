#coding=utf-8

import os
import re
import sys
import zmq
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
import collections
from zmq.eventloop import ioloop, zmqstream
from watchdog.observers import Observer
from watchdog.observers.api import ObservedWatch
from .utils import SystemEventHandler, DatEventHandler, LogEventHandler

VERSION = 0.2

INITIAL_DATA = {
    'cache':{'system':{},'log':{},'dat':{}},
    'harvester_starttime':datetime.datetime.utcnow(),
    'msgs':[],
    'msgs_not_confirmed':{},
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
fh.setLevel(logging.DEBUG)
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


class DictEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S")
        elif isinstance(obj,collections.deque):
            return list(obj)
        else:
            return json.JSONEncoder.default(self, obj)

class Harvester(object):
    SLEEP_TIME = 2
    PERSISTENCE = '.harvester.json'
    SERVER_PUSH_PORT = 5051
    SERVER_PULL_PORT = 5052
    CONF = 'harvester.yaml'

    def __init__(self,data=None,logging=True):
        self.uuid = uuid.uuid4().get_hex()
        self._interrupted = False
        self._observer_started = False
        self.load_conf()
        self._context = zmq.Context()
        self._loop = ioloop.IOLoop.instance()
        self.hostname = socket.gethostname()
        self.load_state(data)
        self._connect()
        self._killed = False
        self.root_path = os.path.abspath('.')
        self._main_control_loop = ioloop.PeriodicCallback(
            self._main_control_task,self.SLEEP_TIME*1e3,self._loop)

    def handle_response(self,msg):
        if 'confirm' == msg[0] :
            doc = json.loads(msg[1])
            if doc['hash'] in self.msgs_not_confirmed:
                self.msgs_not_confirmed.pop(doc['hash'])
                logger.debug('confirmed: {0}'.format(doc['hash']))

    def _connect(self):
        self._server_pull_socket = self._context.socket(zmq.PUSH)
        self._server_pull_socket.connect('tcp://{0}:{1}'.format(
            self.conf.get('host','localhost'),
            self.conf.get('port',self.SERVER_PULL_PORT)))

        self._server_push_socket = self._context.socket(zmq.PULL)
        self._server_push_socket.connect('tcp://{0}:{1}'.format(
            self.conf.get('host','localhost'),
            self.conf.get('pull_port',self.SERVER_PUSH_PORT)))

        self.stream = zmqstream.ZMQStream(self._server_push_socket, self._loop)
        self.stream.on_recv(self.handle_response)

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
        try:
            with open(self.PERSISTENCE,'r') as f:
                data = json.loads(f.read())
                stored_version = data.get('version',0.0)
                if stored_version < 0.2:
                    data = convert_from(data,stored_version)
                self.data.update(data)
            logger.info('loaded state from: {0}'.format(self.PERSISTENCE))
        except:
            logger.error('failed to load {0}'.format(self.PERSISTENCE))
        self.msgs = collections.deque(self.data['msgs'])
        self.msgs_not_confirmed = self.data['msgs_not_confirmed']
        self.cache = self.data['cache']
        self.data['version'] = VERSION

    def _create_observer(self):
        self._observer = observer = Observer()
        for handler_type in self.watch:
            if handler_type == 'system':
                Handler = SystemEventHandler
            elif handler_type == 'log':
                Handler = LogEventHandler
            elif handler_type == 'dat':
                Handler = DatEventHandler
            for item in self.watch[handler_type]:
                recursive = item.get('recursive')
                handler = Handler(self.msgs,self.cache,regexes=item['regexes'])
                handler.init(item['path'],recursive)
                observer.schedule(handler,item['path'],recursive=recursive)

    def _send_initial(self,data):
        # base doc
        doc = {
            'project':self.project,
            'root_path':self.root_path,
            'timestamp':datetime.datetime.utcnow(),
            'host':self.hostname,
            'harvester_starttime':self.data['harvester_starttime'],
        }
        # update with harvested data, we do not check if fields are overridden,
        # so be concious
        doc.update(data)
        # jsonify and calculate hash
        msg = json.dumps(doc,cls=DictEncoder)
        msg_hash = hash(msg)
        # insert into reliability dict
        self.msgs_not_confirmed[msg_hash] = msg
        # send the msg
        logger.info('sending: {0} [{1}]'.format(doc['path'],msg_hash))
        self._server_pull_socket.send_multipart(['new_msg',msg])

    def _main_control_task(self):
        while len(self.msgs) > 0:
            self._send_initial(self.msgs.pop())

    def save_state(self):
        try:
            if os.path.isfile(self.PERSISTENCE):
                os.chmod(self.PERSISTENCE,stat.S_IWRITE|stat.S_IRGRP|stat.S_IROTH)
            with open(self.PERSISTENCE,'w') as f:
                f.write(json.dumps(self.data,indent=2,cls=DictEncoder))
            # mark as read only
            os.chmod(self.PERSISTENCE,stat.S_IREAD|stat.S_IRGRP|stat.S_IROTH)
            logger.info('saved state to: {0}'.format(self.PERSISTENCE))
        except KeyboardInterrupt:
            logger.critical('interrupted the save state')

    def teardown(self):
        if len(self.msgs_not_confirmed)==0:
            self._forced_teardown()

    def stop_observer(self):
        if self._observer_started:
            self._observer.stop()
            self._observer_started = False
            logger.info('stopped observers')

    def _forced_teardown(self):
        if not self._killed:
            self._killed = True
            self.save_state()
            self._loop.stop()
            #time.sleep(0.01)
            self._server_pull_socket.close(linger=0)
            self._server_push_socket.close(linger=0)
            self._context.term()

    def oneshot(self):
        self._create_observer()
        self._main_control_task()
        self._register_soft_teardown()
        self._loop.start()

    def observe(self):
        self._create_observer()
        self._observer.start()
        self._observer_started = True
        self._main_control_task()
        self._main_control_loop.start()
        self._loop.start()

    def kill_handler(self,signum,frame):
        logger.info('forcing teardown')
        self._forced_teardown()

    def _register_soft_teardown(self):
        self.teardown()
        if not self._killed:
            self._teardown_loop = ioloop.PeriodicCallback(
                self.teardown,self.SLEEP_TIME*1e3,self._loop)
            self._teardown_loop.start()
            logger.info('started soft teardown')

    def handle_user_input(self,fd,events):
        self._loop.remove_handler(sys.stdin.fileno())
        value = sys.stdin.readline().strip()
        if value in ('y','yes'):
            logger.info('forcing teardown')
            self._forced_teardown()
        else:
            self._register_soft_teardown()
        signal.signal(signal.SIGINT, self.interrupt_handler)

    def interrupt_handler(self,signum,frame):
        self.stop_observer()
        if not self._interrupted:
            self._interrupted = True
            signal.signal(signal.SIGINT, self.kill_handler)
            if len(self.msgs_not_confirmed)>0:
                print('there are unconfirmed msgs, really quit [y/N]:')
                self._loop.add_handler(
                    sys.stdin.fileno(),self.handle_user_input,self._loop.READ)
            else:
                self.teardown()
        else:
            signal.signal(signal.SIGINT, self.kill_handler)
            if len(self.msgs_not_confirmed)>0:
                print('there are still unconfirmed msgs, really quit [y/N]:')
                self._loop.add_handler(
                    sys.stdin.fileno(),self.handle_user_input,self._loop.READ)
            else:
                self.teardown()


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
        signal.signal(signal.SIGINT, harvester.interrupt_handler)
        harvester.observe()

@run.command()
def oneshot():
    try:
        harvester = Harvester()
    except KeyboardInterrupt:
        print('catched keyboard interrupt on initalization')
    else:
        signal.signal(signal.SIGINT, harvester.interrupt_handler)
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

