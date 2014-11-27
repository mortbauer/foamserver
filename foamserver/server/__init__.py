import os
import zmq
import time
import json
import yaml
import click
import signal
import logging
import pymongo
import dictdiffer
from pymongo.son_manipulator import SONManipulator

class FoamServerException(Exception):
    pass

class KeyTransform(SONManipulator):
    """Transforms keys going to database and restores them coming out.
    adapted from gist: https://gist.github.com/8051599.git

    This allows keys with dots in them to be used (but does break searching on
    them unless the find command also uses the transform.

    Example & test:
        # To allow `.` (dots) in keys
        import pymongo
        client = pymongo.MongoClient("mongodb://localhost")
        db = client['delete_me']
        db.add_son_manipulator(KeyTransform(".", "_dot_"))
        db['mycol'].remove()
        db['mycol'].update({'_id': 1}, {'127.0.0.1': 'localhost'}, upsert=True,
                           manipulate=True)
        print db['mycol'].find().next()
        print db['mycol'].find({'127_dot_0_dot_0_dot_1': 'localhost'}).next()

    Note: transformation could be easily extended to be more complex.
    """
    REPLACE = [('.','__dot__'),('$','__dollar__')]

    def __init__(self,replace=[]):
        self.replace = self.REPLACE + replace

    def transform_key(self, key):
        """Transform key for saving to database."""
        for it,rep in self.replace:
            key = key.replace(it,rep)
        return key

    def revert_key(self, key):
        """Restore transformed key returning from database."""
        for it,rep in self.replace:
            key = key.replace(rep,it)
        return key

    def transform_incoming(self, son, collection):
        """Recursively replace all keys that need transforming."""
        for (key, value) in son.items():
            for it,rep in self.replace:
                if it in key:
                    if isinstance(value, dict):
                        son[self.transform_key(key)] = self.transform_incoming(
                            son.pop(key), collection)
                    else:
                        son[self.transform_key(key)] = son.pop(key)
                    # we replace for all anyways so skip loop
                    break
                elif isinstance(value, dict):  # recurse into sub-docs
                    son[key] = self.transform_incoming(value, collection)
        return son

    def transform_outgoing(self, son, collection):
        """Recursively restore all transformed keys."""
        for (key, value) in son.items():
            for it,rep in self.replace:
                if rep in key:
                    if isinstance(value, dict):
                        son[self.revert_key(key)] = self.transform_outgoing(
                            son.pop(key), collection)
                    else:
                        son[self.revert_key(key)] = son.pop(key)
                elif isinstance(value, dict):  # recurse into sub-docs
                    son[key] = self.transform_outgoing(value, collection)
        return son

class FoamServer(object):
    PORT = 5051

    def __init__(self,conffile=None,logging=True):
        try:
            self.client = pymongo.MongoClient('localhost',27017)
        except pymongo.errors.ConnectionFailure:
            raise FoamServerException('couldn\'t connect to mongodb on localhost:27017')
        self._logger = None
        self.load_conf(conffile)
        self.add_logger()
        self.db = self.client['ventilator']
        self.db.add_son_manipulator(KeyTransform())
        self.differ = dictdiffer.DictDiffer()
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:{0}".format(self.PORT))
        self._stop = False
        print("listening on port: {0}".format(self.PORT))
        self.log('info','started and listening on port: {0}'.format(self.PORT))

    def signal_handler(self,*args):
        self.stop()

    def process_msg(self,msg):
        payload = json.loads(msg)
        project = payload['project']
        host = payload['host']
        harvester_starttime = payload['harvester_starttime']
        data = payload['data']
        for d in data:
            base_doc = {
                'project':project,
                'path':d['path'],
                'type':d['type'],
                'host':host,
                'harvester_starttime':harvester_starttime,
            }
            if 'ctime' in d:
                base_doc['ctime'] = d['ctime']
            doc = self.db[d['type']].find_one(base_doc)
            if doc is None:
                doc = base_doc
                if d['type'] == 'log':
                    doc['data'] = {'loglines':{d['timestamp']:d['loglines']}}
                elif d['type'] == 'dat':
                    path_pieces = d['path'].split('/')
                    doc['data'] = {
                        'data':d['data'],
                        'time':d['data'].pop('Time'),
                        'meta':d['meta'],
                        'starttime':float(path_pieces[-2]),
                        'name':path_pieces[-3],
                        'file':path_pieces[-1],
                    }
                elif d['type'] == 'system':
                    doc['data'] = {'doc':d['doc'],'diffs':{}}
                else:
                    self.log('error','type {0} unknown for: {1}'.format(d['type'],base_doc))
                self.db[d['type']].insert(doc)
            else:
                if 'is_new' in d:
                    self.log('warning','data marked as is_new but isn\'t: {0}'.format(base_doc))
                    continue
                if d['type'] == 'log':
                    self.db[d['type']].update(
                        {'_id':doc['_id']},
                        {'$set':{
                            'data.loglines.{0}'.format(d['timestamp']):d['loglines']}}
                    )
                elif d['type'] == 'system':
                    if 'doc' in d:
                        diffs = list(self.differ.diff(doc['data']['doc'],d['doc']))
                    else:
                        diffs = d['diffs']
                    self.db[d['type']].update(
                        {'_id':doc['_id']},
                        {'$set':{'data.diffs.{0}'.format(d['timestamp']):diffs}}
                    )
                elif d['type'] == 'dat':
                    self.db[d['type']].update(
                        {'_id':doc['_id']},
                        {'$push':{'data.{0}'.format(x):{'$each':d['data'][x]} for x in d['data']}}
                    )
                else:
                    self.log('error','type {0} unknown for: {1}'.format(d['type'],base_doc))

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

    def start_loop(self):
        while not self._stop:
            #  Wait for next request from client
            msg = self.socket.recv()
            self.socket.send(json.dumps({'state':'Ok'}))
            self.process_msg(msg)
        self.teardown()

    def teardown(self):
        self.socket.close()
        self.client.close()
        self.context.term()

    def start(self):
        self.start_loop()

    def stop(self):
        self._stop = True

    def log(self,level,msg):
        if self._logger:
            getattr(self._logger,level)(msg)

    def add_logger(self):
        self._logger = logging.getLogger('foamserver')
        self._logger.setLevel(logging.INFO)
        fh = logging.FileHandler(self.logfile)
        fh.setLevel(logging.INFO)
        fm = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(fm)
        self._logger.addHandler(fh)


@click.command()
def main():
    try:
        server = FoamServer()
        server.start()
    except KeyboardInterrupt:
        print('catched keyboard interrupt on initalization')
    except FoamServerException as e:
        print(e)
