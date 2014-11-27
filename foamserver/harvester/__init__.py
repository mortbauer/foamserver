#coding=utf-8

import os
import re
import sys
import zmq
import json
import yaml
import time
import click
import signal
import socket
import datetime
import logging
from watchdog.observers import Observer
from .utils import PyFoamDictEncoder,SystemEventHandler, DatEventHandler, LogEventHandler


class Harvester(object):
    REQUEST_TIMEOUT = 2500
    SLEEP_TIME = 2
    PERSISTENCE = '.harvester.json'
    CONF = 'harvester.yaml'

    def __init__(self,data=None,logging=True):
        self._socket = None
        self._stop = False
        self._started = False
        self._logger = None
        self._oneshot = False
        self.load_conf()
        self.context = zmq.Context()
        self.hostname = socket.gethostname()
        self.server_addr = 'tcp://{0}:{1}'.format(
            self.conf.get('host','localhost'),
            self.conf.get('port',5051))
        self.data = {} if data is None else data
        if logging:
            self.add_logger()
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
        if 'logfile' in self.conf:
            self.logfile = self.conf['logfile']
        else:
            self.logfile = '.harvester.log'

    def load_state(self):
        if not 'msgs' in self.data:
            self.data['msgs'] = []
        try:
            with open(self.PERSISTENCE,'r') as f:
                self.data.update(json.loads(f.read()))
        except:
            self.log('error','failed to load {0}'.format(self.PERSISTENCE))
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
                self.msgs,'system',self.data['system'],logger=self._logger),
            'system',recursive=False)
        #observer.schedule(
            #SystemEventHandler(
                #self.msgs,'0.00000000e+00',self.data['system']),
            #'0.00000000e+00',recursive=False)
        if not 'postProcessing' in self.data:
            self.data['postProcessing'] = {}
        observer.schedule(
            DatEventHandler(
                self.msgs,'postProcessing',self.data['postProcessing'],logger=self._logger),
            'postProcessing',recursive=True)
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
        },cls=PyFoamDictEncoder)
        try:
            self.socket.send(msg)
            res = json.loads(self.socket.recv())
            if res['state'] == 'Ok':
                del self.msgs[:n]
        except zmq.error.Again:
            self.log('error','zmq error, try reconnect')
            self.close_socket()
        except zmq.error.ZMQError as e:
            if self._stop:
                self.log('debug','ignore zmq.error.ZMQError _stop == True')
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
            with open(self.PERSISTENCE,'w') as f:
                f.write(json.dumps(self.data,indent=2,cls=PyFoamDictEncoder))
        except KeyboardInterrupt:
            self.log('critical','interrupted the save state')

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

    def log(self,level,msg):
        if self._logger:
            getattr(self._logger,level)(msg)

    def add_logger(self):
        self._logger = logging.getLogger('harvester_{0}'.format(self.project))
        self._logger.setLevel(logging.CRITICAL)
        fh = logging.FileHandler(self.logfile)
        fh.setLevel(logging.DEBUG)
        fm = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(fm)
        self._logger.addHandler(fh)


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

