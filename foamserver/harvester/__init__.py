#coding=utf-8

import os
import re
import sys
import zmq
import stat
import json
import yaml
import time
import click
import signal
import socket
import datetime
import logging
from watchdog.observers import Observer
from .utils import SystemEventHandler, DatEventHandler, LogEventHandler

fmt = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.ERROR,format=fmt)
logger = logging.getLogger('harvester')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('.harvester.log')
fh.setLevel(logging.INFO)
fm = logging.Formatter(fmt)
fh.setFormatter(fm)
logger.addHandler(fh)

class DictEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)

class Harvester(object):
    REQUEST_TIMEOUT = 2500
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
        self.data = {} if data is None else data
        self.load_state()
        self.create_observer()

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

    def load_state(self):
        if not 'msgs' in self.data:
            self.data['msgs'] = []
        try:
            with open(self.PERSISTENCE,'r') as f:
                self.data.update(json.loads(f.read()))
        except:
            logger.error('failed to load {0}'.format(self.PERSISTENCE))
        finally:
            if not 'harvester_starttime' in self.data:
                self.data['harvester_starttime'] = datetime.datetime.utcnow()
            self.msgs = self.data['msgs']

    def create_observer(self):
        self.observer = observer = Observer()
        if not 'system' in self.data:
            self.data['system'] = {}
        observer.schedule(
            SystemEventHandler(
                self.msgs,'system',self.data['system']),
            'system',recursive=False)
        #observer.schedule(
            #SystemEventHandler(
                #self.msgs,'0.00000000e+00',self.data['system']),
            #'0.00000000e+00',recursive=False)
        if not 'postProcessing' in self.data:
            self.data['postProcessing'] = {}
        observer.schedule(
            DatEventHandler(
                self.msgs,'postProcessing',self.data['postProcessing']),
            'postProcessing',recursive=True)
        for path in self.conf.get('postProcessing',[]):
            observer.schedule(
                DatEventHandler(
                    self.msgs,path,self.data['postProcessing']),
                path,recursive=True)
        if not 'logs' in self.data:
            self.data['logs'] = {}
        observer.schedule(
            LogEventHandler(
                self.msgs,'./',self.data['logs']),'./',recursive=True)

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

