import os
import sys
import zmq
import json
import time
import yaml
import h5py
import click
import numpy
import redis
import pymongo
import gevent
import signal
import logging
import datetime
import traceback
import collections
import gevent.select
from mpi4py import MPI
from json import dumps, loads

#fmt = '%(message)s'
fmt = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.ERROR,format=fmt)
logger = logging.getLogger('foamserver_server')
logger.setLevel(logging.INFO)
fm = logging.Formatter(fmt)

def proc_line(line,start_time,n_iter,n_outer_corr,n_corr,n_ortho):
    fields = line.split(',')
    solver, rest = fields[0].split(':')
    key = rest.split()[-1]
    dd = (solver,float(fields[1].strip().split('=')[-1]),
        float(fields[2].strip().split('=')[-1]),
        int(fields[3].strip().split()[-1]),start_time,n_iter,n_outer_corr,n_corr,n_ortho)
    return key,dd,fields[0]

class DictEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)

class BaseProcessor(object):
    def __call__(self,doc,*args):
        try:
            return self.TYPE, doc, self.process(doc,*args)
        except Exception as e:
            print(traceback.format_exc())

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

class BaseApp(object):
    NAME = ''
    CONF = {}

    def __init__(self,conffile=None,debug=False,loglevel='error'):
        self._shutdown = False
        self.redis = redis.StrictRedis()
        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(loglevel.upper())
        self.load_conf(conffile)
        self.add_logging_file_handler()
        self.raw_queue = 'foamserver_raw_queue'

    @property
    def name(self):
        return 'foamserver_{0}'.format(self.NAME)

    def make_redis_name(self,name):
        return 'foamserver_{0}_{1}'.format(self.NAME,name)


    def start(self):
        signal.signal(signal.SIGINT, self.interrupt_handler)

    def load_conf(self,confdir):
        if confdir is None:
            if 'XDG_CONFIG_HOME' in os.environ:
                confdir = os.path.join(
                    os.environ['XDG_CONFIG_HOME'],'foamserver')
            else:
                confdir = os.path.join(os.environ['HOME'],'.foamserver')
        conffile = os.path.join(confdir,'%s.yaml'%self.NAME)
        try:
            os.makedirs(confdir)
        except:
            pass
        if not os.path.isfile(conffile):
            with open(conffile,'w') as f:
                f.write(yaml.dump(self.CONF))
        with open(conffile,'r') as f:
            self.conf = yaml.load(f.read())
        if self.conf is None:
            self.conf= {}

    def add_logging_file_handler(self):
        logfile = self.conf.get('logfile')
        if logfile:
            fh = logging.FileHandler(logfile)
            fh.setLevel(logging.INFO)
            fh.setFormatter(fm)
            logger.addHandler(fh)


    def interrupt_handler(self,signum,frame):
        print('catched interrupt, shutting down')
        self._shutdown = True

class FoamPostProcessorHDF5(BaseApp):
    NAME = 'postpro'
    CONF = {
        'datafile':'foamserver_data.h5',
    }
    doc = collections.namedtuple(
        'doc',['host','project','uuid','type','path','initially'])

    var_row = [
        ('solver','S12'),('initial','f8'),('final','f8'),('n_lin_iter','i4'),
        ('time','f8'),('n_iter','i8'),('n_outer_corr','i8'),('n_corr','i8'),('n_ortho','i8')]

    tsce_row = [
        ('sum local','f8'),('global','f8'),('cumulative','f8'),
        ('n_iter','i8'),('n_outer_corr','i8'),('n_corr','i8')]

    main_row = [
        ('time','f8'),('execution_time','f8'),('clock_time','f8'),
        ('convergence','S40'),('delta_t','f8'),('cfl_mean','f8'),
        ('cfl_max','f8'),('n_iter','i8'),('n_outer_corr','i8'),('n_corr','i8'),('n_ortho','i8')]

    reread_row = [('n_iter','i8'),('name','S20'),('path','S150')]

    def __init__(self,**kwargs):
        super(FoamPostProcessorHDF5,self).__init__(**kwargs)
        self.currently_processing = self.make_redis_name('currently_processing')
        self._failed = self.make_redis_name('failed')
        self.conn = pymongo.MongoClient()
        self.db = self.conn['foamserver_postpro']

    def process_loop(self):
        while not self._shutdown:
            d_id = self.redis.lpop(self.raw_queue)
            if d_id is None:
                gevent.sleep(1.)
            else:
                if self.redis.sadd(self.currently_processing,d_id) and not \
                        self.redis.sismember(self._failed,d_id):
                    self.process_doc(d_id)
                    self.redis.srem(self.currently_processing,d_id)

    def process_recompute_loop(self,queue):
        while not self._shutdown:
            d_id = self.redis.spop(queue)
            if d_id is None:
                break
            else:
                if self.redis.sadd(self.currently_processing,d_id):
                    self.redis.delete(d_id)
                    del self.datastore[self.redisid_to_h5id(d_id)]
                    logger.warn('deleted: %s from redis and datastore',d_id)
                    self.process_doc(d_id)
                    self.redis.srem(self.currently_processing,d_id)

    @staticmethod
    def redisid_to_h5id(redis_id):
        return redis_id[5:].replace('::','/')

    def process_failed_loop(self,failed=''):
        while not self._shutdown:
            d_id = self.redis.spop('::'.join((self._failed,str(failed))))
            if d_id is None:
                break
            else:
                if self.redis.sadd(self.currently_processing,d_id):
                    if not self.redis.hget(d_id,'n_curr'):
                        self.redis.hdel(d_id,'header')
                        self.process_doc(d_id)
                    else:
                        logger.error('i\'m scared to process %s since there is already data',d_id)
                    self.redis.srem(self.currently_processing,d_id)

    def process_doc(self,d_id):
        d = self.doc(*d_id[5:].split('::'))
        h5_id = d_id[5:].replace('::','/')
        processor = getattr(self,'process_%s'%d.type)
        #print('poping of %s'%d_id)
        if not h5_id in self.datastore:
            group = self.datastore.create_group(h5_id)
            # create also a shortcut to the new data
            # project wide
            latest = '/'.join((d.host,d.project,'latest'))
            if latest in self.datastore:
                del self.datastore[latest]
            self.datastore[latest] = self.datastore['/'.join((d.host,d.project,d.uuid))]
            # doc wide
            latest = '/'.join((group.parent.name,'latest'))
            if latest in self.datastore:
                del self.datastore[latest]
            self.datastore[latest] = self.datastore[group.name]
        else:
            group = self.datastore[h5_id]
        msgs_id = '::'.join((d_id,'msgs'))
        while not self._shutdown:
            n = self.redis.hget(d_id,'next')
            if n is None:
                n = '0'
            msg = self.redis.hget(msgs_id,n)
            if not msg:
                break
            else:
                meta,payload_msg = loads(msg)
                payload = loads(payload_msg)
                group = self.datastore[h5_id]
                t0 = time.time()
                try:
                    # store into mongo
                    d_doc = d._asdict()
                    d_doc['n'] = n
                    if d.type == 'dat':
                        meta['starttime'] = self.get_starttime_from_dat_path(d.path)
                    self.db.archive.insert(
                        {'_id':d_doc,'meta':meta,'payload':payload})
                except pymongo.errors.DuplicateKeyError:
                    logger.debug('msg already in archive: %s',d_id)
                try:
                    processor(group,d_id,meta,payload)
                except Exception as e:
                    logger.critical('failed to process: %s::%s \n %s',d_id,n,e)
                    print(traceback.format_exc())
                    n_failed = self.redis.hincrby(d_id,'failed')
                    self.redis.sadd('::'.join((self._failed,str(n_failed))),d_id)
                    break
                else:
                    self.redis.hincrby(d_id,'next')
                    logger.debug('processed %s::%s in %s sec',d_id,n,time.time()-t0)
                    #self.redis.hdel(msgs_id,n)
                    self.datastore.flush()

    def start(self,failed=None,reprocess=None):
        super(FoamPostProcessorHDF5,self).start()
        logger.info("started server")
        filename = self.conf.get('datafile','%s.h5'%self.name)
        try:
            #self.datastore = h5py.File(
                #filename,'r+',driver='mpio',comm=MPI.COMM_WORLD)
            self.datastore = h5py.File(filename,'r+')
        except IOError:
            #self.datastore = h5py.File(
                #filename,'w',driver='mpio',comm=MPI.COMM_WORLD)
            self.datastore = h5py.File(filename,'w')
            logger.debug('creating file "%s"',filename)
        #gevent.joinall(
            #gevent.spawn(self.process_loop),
        #])
        #self.datastore.atomic = True
        if failed is not None:
            self.process_failed_loop(failed)
        elif reprocess is not None:
            self.process_recompute_loop(reprocess)
        else:
            self.process_loop()
        self.datastore.close()

    def process_log(self,group,d_id,meta,lines):
        self.redis.hsetnx(d_id,'n_runs',0)
        self.redis.hsetnx(d_id,'n_iter',0)
        self.redis.hsetnx(d_id,'n_outer_corr',0)
        self.redis.hsetnx(d_id,'n_corr',0)
        self.redis.hsetnx(d_id,'n_ortho',0)
        self.redis.hsetnx(d_id,'n_corr_max',0)
        self.redis.hsetnx(d_id,'n_ortho_max',0)
        self.redis.hsetnx(d_id,'start_time',0)
        self.redis.hsetnx(d_id,'executable','')
        self.redis.hsetnx(d_id,'cfl_mean',0)
        self.redis.hsetnx(d_id,'cfl_max',0)
        self.redis.hsetnx(d_id,'deltaT',0)
        n_runs = self.redis.hget(d_id,'n_runs')
        executable = self.redis.hget(d_id,'executable')
        while len(lines):
            if lines[0].startswith('Build  :'):
                if 'run%s'%n_runs not in group:
                    sg = group.create_group('run%s'%n_runs)
                else:
                    sg = group['run%s'%n_runs]
                executable = self.process_log_header(sg,d_id,meta,lines)
            elif 'simple' in executable or 'pimple' in executable or 'sonic' in executable:
                self.process_log_imple(group['run%s'%n_runs],d_id,meta,lines)
                executable = self.redis.hget(d_id,'executable')
                n_runs = self.redis.hget(d_id,'n_runs')
            else:
                lines.pop(0)
                if len(executable):
                    logger.error('unknown exec: %s',executable)

    def process_log_header(self,group,d_id,meta,lines):
        # get the executable info
        d = {}
        for i in range(8):
            line = lines.pop(0)
            key,value = line.strip().split(':',1)
            d[key.strip()] = value.strip()
        # get the host computers
        if int(value) > 1:
            key = lines.pop(0).strip(' :\n')
            lines.pop(0)
            lines.pop(0)
            slaves = []
            for i in range(int(value)-1):
                slaves.append(lines.pop(0).strip(' \n"'))
            lines.pop(0)
            d[key] = '::'.join(slaves)
        self.redis.hset(d_id,'executable_info',dumps(d))
        self.redis.hset(d_id,'executable',d['Exec'].lower())
        # write the info as attrs
        for k in d:
            group.attrs[k] = d[k]
        return d['Exec'].lower()

    def process_log_imple(self,group,d_id,meta,lines):
        d = {}
        tsceinfo = []
        maininfo = []
        reread = []
        n_iter = int(self.redis.hget(d_id,'n_iter'))
        n_outer_corr = int(self.redis.hget(d_id,'n_outer_corr'))
        n_corr = int(self.redis.hget(d_id,'n_corr'))
        n_corr_max = int(self.redis.hget(d_id,'n_corr_max'))
        n_ortho = int(self.redis.hget(d_id,'n_ortho'))
        n_ortho_max = int(self.redis.hget(d_id,'n_ortho_max'))
        start_time = float(self.redis.hget(d_id,'start_time'))
        convergence_info = self.redis.hget(d_id,'convergence_info')
        cfl_mean = float(self.redis.hget(d_id,'cfl_mean'))
        cfl_max = float(self.redis.hget(d_id,'cfl_max'))
        deltaT =float( self.redis.hget(d_id,'deltaT'))
        while len(lines):
            line = lines.pop(0).strip()
            if line.startswith('Time ='):
                key, value = line.split(' = ')
                start_time = float(value)
            if 'Solving for ' in line:
                n_ortho = 0
                key,dd,start = proc_line(
                    line,start_time,n_iter,n_outer_corr,n_corr,n_ortho)
                if not key in d:
                    d[key] = []
                d[key].append(dd)
                while len(lines):
                    if not lines[0].startswith(start):
                        break
                    n_ortho += 1
                    key,dd,start = proc_line(
                        lines.pop(0),start_time,n_iter,n_outer_corr,n_corr,n_ortho)
                    d[key].append(dd)
            elif line.startswith('Courant Number mean:'):
                fields = line.split(': ')
                cfl_mean = float(fields[1].split()[0])
                cfl_max = float(fields[2])
            elif line.startswith('deltaT ='):
                deltaT = float(line.split(' = ')[1])
            elif line.startswith('time step continuity errors'):
                n_corr +=1
                n_ortho_max = n_ortho
                n_ortho = 0
                key, rest = line.split(':')
                fields = rest.split(',')
                tsceinfo.append((float(fields[0].split('=')[1].strip()),
                      float(fields[1].split('=')[1].strip()),
                      float(fields[2].split('=')[1].strip()),n_iter,n_outer_corr,n_corr))
            elif line.startswith('PIMPLE: iteration'):
                n_corr_max = n_corr
                n_corr = 0
                n_outer_corr += 1
            elif line.startswith('PIMPLE: '):
                convergence_info = line.split(':')[1].strip()
            elif 'Re-reading object' in line:
                name,filepath = line[18:].split(' from file ')
                reread.append((n_iter,name,filepath.strip('"')))
            elif line.startswith('ExecutionTime'):
                fields = line.split('  ')
                maininfo.append((
                    start_time,
                    float(fields[0].split(' = ')[1][:-2]),
                    float(fields[0].split(' = ')[1][:-2]),
                    convergence_info,deltaT,cfl_mean,cfl_max,n_iter,n_outer_corr,
                    n_corr_max,n_ortho_max
                ))
                n_corr_max = 0
                n_ortho_max = 0
                n_outer_corr = 0
                n_iter +=1
            elif line.startswith('End'):
                self.redis.hset(d_id,'executable','')
                self.redis.hincrby(d_id,'n_runs')
                n_iter = 0
                n_outer_corr = 0
                break

        # update the generic keys
        for key in d:
            n = len(d[key])
            if not key in group:
                ds = group.create_dataset(
                    key,dtype=self.var_row,shape=(n,),
                    chunks=True,maxshape=(None,))
            else:
                ds = group[key]
                ds.resize((ds.shape[0]+n,))
            ds[-n:] = d[key]
        # update tsce and main
        for key,dtype,val in (
            ('main',self.main_row,maininfo),
            ('reread',self.reread_row,reread),
            ('tsc',self.tsce_row,tsceinfo)):
            n = len(val)
            if n > 0:
                if not key in group:
                    ds = group.create_dataset(
                        key,(n,),dtype,chunks=True,maxshape=(None,))
                else:
                    ds = group[key]
                    ds.resize((ds.shape[0]+n,))
                ds[-n:] = val

        # save the state
        self.redis.hset(d_id,'n_iter',n_iter)
        self.redis.hset(d_id,'n_corr',n_corr)
        self.redis.hset(d_id,'n_corr_max',n_corr_max)
        self.redis.hset(d_id,'n_outer_corr',n_outer_corr)
        self.redis.hset(d_id,'n_ortho',n_ortho)
        self.redis.hset(d_id,'n_ortho_max',n_ortho_max)
        self.redis.hset(d_id,'n_corr',n_corr)
        self.redis.hset(d_id,'start_time',start_time)
        self.redis.hset(d_id,'convergence_info',convergence_info)
        self.redis.hset(d_id,'cfl_mean',cfl_mean)
        self.redis.hset(d_id,'cfl_max',cfl_max)
        self.redis.hset(d_id,'deltaT',deltaT)

    def process_system(self,group,d_id,meta,lines):
        pass

    def process_dat(self,group,d_id,meta,lines):
        size,ndim,header,cols = self.process_dat_collective(group,d_id,meta,lines)
        i = 0
        if ndim > 1:
            for i,line in enumerate(lines):
                time,fields = line.strip().split('\t')
                cols[0][i] = float(time)
                for j,main_field in enumerate(fields.split(') ')):
                    for k,x in enumerate(main_field.strip('()').split()):
                        cols[j+1][i,k] = float(x)
        else:
            for i,line in enumerate(lines):
                if '\t' in line:# openfoam insert \t but swak4Foam doesn't
                    fields = line.strip().split('\t')
                else:
                    fields = line.split()
                cols[0][i] = float(fields[0])
                for j,x in enumerate(fields[1:]):
                    cols[j+1][i] = float(x)
        self.redis.hincrby(d_id,'n_curr',i+1)
        # transfer the data from our temporary numpy arrays to hdf5
        for i,(x,n_cols) in enumerate(header):
            group[x][-size:] = cols[i]
            #print(x,i,cols[i])

    def process_dat_collective(self,group,d_id,meta,lines):
        # take the header from redis if there is
        header_msg = self.redis.hget(d_id,'header')
        if header_msg is None:
            header,nh = self.process_dat_header(d_id,lines)
        else:
            header = loads(header_msg)
            nh = int(self.redis.hget(d_id,'nheadlines'))
        # create the groups/dataset in the hdf5 if not there otherwise resize
        n_max = meta['n_max']-(nh-1)
        cols = []
        n_cols_max = 0
        for key,n_cols in header:
            if n_cols > n_cols_max:
                n_cols_max = n_cols
            if key in group:
                curr_size = group[key].shape[0]
                size = n_max-curr_size
                group[key].resize((n_max,n_cols)if n_cols>1 else (n_max,))
            else:
                group.create_dataset(
                    key,(n_max,n_cols)if n_cols>1 else (n_max,),
                    'f8',chunks=True,maxshape=(None,n_cols)if n_cols>1 else (None,))
                size = n_max
            cols.append(numpy.empty((size,n_cols)if n_cols>1 else (size,)))

        #print(d_id,size,n_max,n_curr,shape,header)
        return size,n_cols_max,header,cols

    def process_dat_header(self,d_id,lines):
        headlines = []
        while lines and lines[0].strip().startswith('#'):
            headlines.append(lines.pop(0))
        if len(headlines)==3 and 'Forces' in headlines[0]:
            header = []
            comment,time,fields = headlines[2].split(' ',2)
            header.append((time,1))
            for main_field in fields.strip().split(') '):
                key,minor_fields = main_field.split('(')
                for minor in minor_fields.split():
                    header.append(('/'.join((key,minor.strip(')'))),3))
        else:
            if '\t' in headlines[-1]:# openfoam insert \t but swak4Foam doesn't
                header = [(x,1) for x in headlines[-1].strip('\n# ').split('\t')]
            else:
                header = [(x,1) for x in headlines[-1].strip('\n# ').split()]
        nh = len(headlines)
        self.redis.hset(d_id,'header',dumps(header))
        self.redis.hset(d_id,'nheadlines',nh)
        return header,nh

    def get_starttime_from_dat_path(self,path):
        dirname,filename = os.path.split(path)
        basename,ext = os.path.splitext(filename)
        res = basename.split('_')
        if len(res)>1:
            try:
                return float(res[1])
            except:
                return float(os.path.split(dirname)[1])
        else:
            return float(os.path.split(dirname)[1])

class FoamServer(BaseApp):
    SERVER_PUSH_PORT = 5051
    SERVER_PULL_PORT = 5052
    NAME = 'server'

    def __init__(self,**kwargs):
        super(FoamServer,self).__init__(**kwargs)
        self._p_store = self.make_redis_name('project_store')
        self.context = zmq.Context()
        self.s_sock = self.context.socket(zmq.PUSH)
        self.r_sock = self.context.socket(zmq.PULL)
        self.msg_counter = 0

    def handle_msg(self,doc_msg,payload_msg):
        doc = loads(doc_msg)
        d = doc['doc']
        p = doc['project']
        p_id = '::'.join((p['host'],p['project'],p['uuid']))
        d_id = '::'.join(
            (d['type'],d['path'],d['initially_tracked']))
        f_id = '::'.join((p_id,d_id))
        # insert the project meta data
        if self.redis.hsetnx(self._p_store,p_id,dumps(p)):
            logger.debug('added new project: %s',p_id)
        # store the doc meta
        doc_id = '::'.join(('doc',f_id))
        if self.redis.sadd('::'.join(('project',p_id)),d_id):
            logger.debug('added new doc: %s to %s',d_id,p_id)
        # append the msg to the doc
        if not self.redis.hset(
            '::'.join((doc_id,'msgs')),d['msg_number'],dumps((d,payload_msg))):
            logger.error('failed to insert doc:%s',doc_id)
        else:
            logger.debug('inserted %s',doc_id)
        # push the msg id to the raw_queue
        self.redis.rpush(self.raw_queue,doc_id)
        # send response
        self.s_sock.send_multipart(
            ['confirm',doc_msg,dumps(hash(payload_msg))])

    def receive_loop(self):
        while not self._shutdown:
            try:
                msg = self.r_sock.recv_multipart(flags=zmq.NOBLOCK)
            except zmq.ZMQError:
                gevent.sleep(1.)
            else:
                if 'new' == msg[0] :
                    self.handle_msg(msg[1],msg[2])
                else:
                    gevent.sleep(1.)

    def connect(self):
        self.s_sock.bind("tcp://*:%s"%self.SERVER_PUSH_PORT)
        self.r_sock.bind("tcp://*:%s"%self.SERVER_PULL_PORT)

    def start(self):
        super(FoamServer,self).start()
        self.connect()
        logger.info("started server")
        gevent.joinall([
            gevent.spawn(self.receive_loop),
        ])

@click.group()
def run():
    pass

@run.command()
@click.option('--debug/--no-debug')
@click.option('--loglevel',default='error')
def server(debug=False,loglevel='error'):
    server = FoamServer(debug=debug,loglevel=loglevel)
    server.start()

@run.command()
@click.option('--failed')
@click.option('--reprocess')
@click.option('--debug',is_flag=True)
@click.option('--loglevel',default='error')
@click.option('--profile',is_flag=True)
def processor(debug,loglevel,profile,failed,reprocess):
    server = FoamPostProcessorHDF5(debug=debug,loglevel=loglevel)
    if profile:
        import cProfile
        import pstats
        cProfile.runctx('server.start()',None,locals(),'restats')
        p = pstats.Stats('restats')
        p.strip_dirs().sort_stats('cumulative').print_stats(20)
    else:
        server.start(failed=failed,reprocess=reprocess)

def from_mongo_to_redis():
    for x in db.archive.find({'_id.host':'b200','_id.type':'log'}):
        d = x['_id']
        msg = json.dumps((x['meta'],x['payload']))
        _id = '::'.join(('doc',d['host'],d['project'],d['uuid'],d['type'],d['path'],d['initially'],'msgs'))
        r.hset(_id,d['n'],msg)
