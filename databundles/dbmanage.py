"""Main script for the databaundles package, providing support for creating
new bundles

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from __future__ import print_function
import os.path
import yaml
import shutil
from databundles.run import  get_runconfig
from databundles import __version__

def prt(template, *args, **kwargs):
    print(template.format(*args, **kwargs))

def prtl(template, *args, **kwargs):
    # would like to use print( ..., end='\r') here, but it doesn't seem to work. 
    import sys
    sys.stdout.write('%s\r' % template.format(*args, **kwargs) )
    sys.stdout.flush()


def err(template, *args, **kwargs):
    import sys
    print("ERROR: "+template.format(*args, **kwargs))
    sys.exit(1)

def warn(template, *args, **kwargs):
    import sys
    print("WARN: "+template.format(*args, **kwargs))


class Progressor(object):

    start = None
    last = None
    freq = 1

    def __init__(self, message='Download'):
        import time
        from collections import deque
        self.start = time.clock()
        self.message = message
        self.rates = deque(maxlen=10)
        

    def progress(self, i, n):
        import curses
        import time
        
        import time
        now = time.clock()

        if not self.last:
            self.last = now
        
        if now - self.last > self.freq:
            diff = now - self.start 
            i_rate = float(i)/diff
            self.rates.append(i_rate)
            
            if len(self.rates) > self.rates.maxlen/2:
                rate = sum(self.rates) / len(self.rates)
                rate_type = 'a'
            else:
                rate = i_rate
                rate_type = 'i'

            prtl("{}: Compressed: {} Mb. Downloaded, Uncompressed: {:6.2f}  Mb, {:5.2f} Mb / s ({})",
                 self.message,int(int(n)/(1024*1024)),round(float(i)/(1024.*1024.),2), round(float(rate)/(1024*1024),2), rate_type)
            
            self.last = now
            



def install_command(args, rc, src):
    import yaml, pkgutil
    import os
    from databundles.run import RunConfig as rc

    if args.subcommand == 'config':

        if not args.force and  os.path.exists(rc.ROOT_CONFIG):
            orig = True
            with open(rc.ROOT_CONFIG) as f:
                contents = f.read()
        else:
            orig = False
            contents = pkgutil.get_data("databundles.support", 'databundles.yaml')
            
        d = yaml.load(contents)

        if args.root:
            d['filesystem']['root_dir'] = args.root
        
        s =  yaml.dump(d, indent=4, default_flow_style=False)
        
        if args.prt:
            prt(s)
        else:
            with open(rc.ROOT_CONFIG,'w') as f:
                f.write(s)

def warehouse_command(args, rc, src):
    from databundles.warehouse import new_warehouse

    if args.is_server:
        config  = src
    else:
        config = rc

    w = new_warehouse(config.warehouse(args.name))

    globals()['warehouse_'+args.subcommand](args, w,config)

   
def warehouse_info(args, w,config):
    
    prt("Warehouse Info")
    prt("Name:     {}",args.name)
    prt("Class:    {}",w.__class__)
    prt("Database: {}",w.database.dsn)
    prt("Library : {}",w.library.dsn)

 
def warehouse_drop(args, w,config):
    
    w.database.enable_delete = True
    w.drop()
   
from warehouse import ResolverInterface
class Resolver(ResolverInterface):
    
    def __init__(self, library):
    
        self.library = library
    
    def get(self, name):
        bundle = self.library.get(name)
        
        if bundle.partition:
            return bundle.partition
        else:
            return bundle
    
    def get_ref(self, name):
        return self.library.get_ref(name)

    def url(self, name):
        dsi = self.library.remote.get_ref(name)

        if not dsi:
            return None

        if dsi['ref_type'] == 'partition':
            return dsi['partitions'].items()[0][1]['url']
        else:
            return dsi['dataset']['url']

     

    
def warehouse_install(args, w,config):
    import library
    from functools import partial
    from databundles.util import init_log_rate
    
    if not w.exists():
        w.create()

    l = library.new_library(config.library(args.library))

    
    w.resolver = Resolver(l)
    
    def progress_cb(lr, type_,name,n):
        if n:
            lr("Warehouse Install: {} {}: {}".format(type, name, n))
        else:
            prt("Warehouse Install: {} {}",type_, name)

    lr = init_log_rate(2000)
    
    w.progress_cb = partial(progress_cb, lr) 

    w.install_by_name(args.term )

def library_command(args, rc, src):
    import library

    if args.is_server:
        config  = src
    else:
        config = rc
    
    l = library.new_library(config.library(args.name))

    globals()['library_'+args.subcommand](args, l,config)


def library_init(args, l, config):

    l.database.create()

def library_backup(args, l, config):

    import tempfile

    if args.file:
        backup_file = args.file
        is_temp = False
    else:
        tfn = tempfile.NamedTemporaryFile(delete=False)
        tfn.close()
    
        backup_file = tfn.name+".db"
        is_temp = True

    if args.date:
        from datetime import datetime
        date = datetime.now().strftime('%Y%m%dT%H%M')
        parts = backup_file.split('.')
        if len(parts) >= 2:
            backup_file = '.'.join(parts[:-1]+[date]+parts[-1:])
        else:
            backup_file = backup_file + '.' + date

    prt('{}: Starting backup', backup_file)

    l.database.dump(backup_file)

    if args.cache:
        dest_dir = l.cache.put(backup_file,'_/{}'.format(os.path.basename(backup_file)))
        is_temp = True
    else:
        dest_dir = backup_file

    if is_temp:
        os.remove(backup_file)

        
    prt("{}: Backup complete", dest_dir)
        
def library_restore(args, l, config, *kwargs):

    if args.dir:
      
        if args.file:
            # Get the last file that fits the pattern, sorted alpha, with a date inserted
            from datetime import datetime
            import fnmatch
            
            date = '*' # Sub where the date will be  
            parts = args.file.split('.')
            if len(parts) >= 2:
                pattern = '.'.join(parts[:-1]+[date]+parts[-1:])
            else:
                import tempfile
                tfn = tempfile.NamedTemporaryFile(delete=False)
                tfn.close()
    
                backup_file = tfn.name+".db"
                pattern = backup_file + '.' + date
                
            files = sorted([ f for f in os.listdir(args.dir) if fnmatch.fnmatch(f,pattern) ])
    
        else:
            # Get the last file, by date. 
            files = sorted([ f for f in os.listdir(args.dir) ], 
                           key=lambda x: os.stat(os.path.join(args.dir,x))[8])
    
    
        backup_file = os.path.join(args.dir,files.pop())
        
    elif args.file:
        backup_file = args.file
    
    
    # Backup before restoring. 
    
    args = type('Args', (object,),{'file':'/tmp/before-restore.db','cache': True, 
                                   'date': True, 'is_server': args.is_server, 'name':args.name, 
                                   'subcommand': 'backup'})
    library_backup(args, l, config)
    
    prt("{}: Restoring", backup_file)
    l.clean(add_config_root=False)
    l.restore(backup_file)
   
def library_server(args, l, config):
    from util import daemonize

    from databundles.server.main import production_run, local_run

    def run_server(args, config):
        production_run(config.library(args.name))
    
    if args.daemonize:
        daemonize(run_server, args,  config)
    elif args.test:
        local_run(config.library(args.name))
    else:
        production_run(config.library(args.name))
        
def library_drop(args, l, config):   

    prt("Drop tables")
    l.database.enable_delete = True
    l.database.drop()

def library_clean(args, l, config):

    prt("Clean tables")
    l.database.clean()
        
def library_purge(args, l, config):

    prt("Purge library")
    l.purge()
      
def library_rebuild(args, l, config):  

    prt("Rebuild library")
    l.database.enable_delete = True
    if args.remote:
        l.remote_rebuild()
    else:
        l.rebuild()
        
def library_list(args, l, config):    

    if not args.term:

        for ident in sorted(l.list(), key=lambda x: x['vname']):
            prt("{:2s} {:10s} {}", ''.join(ident['location']), ident['vid'], ident['vname'])
    else:
        library_info(args, l, config, list_all=True)    
 
def library_delete(args, l, config):   
    
    name = args.term
    
    b = l.get(name)
    
    if not b:
        err("Didn't find")
    
    if b.partition:
        k =  b.partition.identity.cache_key
        prt("Deleting partition {}",k)
 
        l.cache.remove(k, propagate = True)
        
    else:
        
        for p in b.partitions:
            k =  p.identity.cache_key
            prt("Deleting partition {}",k)
            l.cache.remove(k, propagate = True)            
        
        k = b.identity.cache_key
        prt("Deleting bundle {}", k)
        l.remove(b)  
    
    
    
    

    

def library_info(args, l, config, list_all=False):    

    if args.term:

        d,p = l.get_ref(args.term)

        if not d:
            err("Failed to find record for: {}", args.term)
            return 
                
        _print_info(l,d,p, list_partitions=list_all)
         
        config = l.config(args.term)

        
    else:

        prt("Library Info")
        prt("Name:     {}",args.name)
        prt("Database: {}",l.database.dsn)
        prt("Cache:    {}",l.cache)
        prt("Remote:   {}",l.remote if l.remote else 'None')

    
def library_push(args, l, config):

    if args.force:
        state = 'all'
    else:
        state = 'new'
    
    files_ = l.database.get_file_by_state(state)
    if len(files_):
        prt("-- Pushing to {}",l.remote)
        for i, f in enumerate(files_):
            prt("Pushing: {}",f.path)
            try:
                l.push(f)
            except Exception as e:
                prt("Failed: {}",e)
                raise
                
def library_files(args, l, config):

    files_ = l.database.get_file_by_state(args.file_state)
    if len(files_):
        prt("-- Display {} files",args.file_state)
        for f in files_:
            prt("{0:11s} {1:4s} {2}",f.ref,f.state,f.path)
      

            
def library_find(args, l, config):
    return _find(args, l, config, False)

def _find(args, l, config, remote):

    from databundles.library import QueryCommand

    terms = []
    for t in args.term:
        if ' ' in t or '%' in t:
            terms.append("'{}'".format(t))
        else:
            terms.append(t)


    qc = QueryCommand.parse(' '.join(terms))
    
    prt("Query: {}", qc)
    
    if remote:
        identities = l.remote_find(qc)
    else:
        identities = l.find(qc)

    try: first = identities[0]
    except: first = None
    
    if not first:
        return
    
    t = ['{id:<14s}','{vname:20s}']
    header = {'id': 'ID', 'vname' : 'Versioned Name'}
    
    multi = False
    if 'column' in first:
        multi = True
        t.append('{column:12s}')
        header['column'] = 'Column'

    if 'table' in first:
        multi = True
        t.append('{table:12s}')
        header['table'] = 'table'

    if 'partition' in first:
        multi = True
        t.append('{partition:50s}')
        header['partition'] = 'partition'
        
    ts = ' '.join(t)
    
    dashes = { k:'-'*len(v) for k,v in header.items() }
   
    prt(ts, **header) # Print the header
    prt(ts, **dashes) # print the dashes below the header
   
    last_rec = None
    first_rec_line = True
    for r in identities:

        if not last_rec or last_rec['id'] != r['identity']['vid']:
            rec = {'id': r['identity']['vid'], 'vname':r['identity']['vname']}
            last_rec = rec
            first_rec_line = True
        else:
            rec = {'id':'', 'vname':''}
   
        if 'column' in r:
            rec['column'] = ''
            
        if 'table' in r:
            rec['table'] = ''

        if 'partition' in r:
            rec['partition'] = ''
           
        if multi and first_rec_line:
            prt(ts, **rec)
            rec = {'id':'', 'vname':''}
            first_rec_line = False
           
        if 'column' in r:
            rec['id'] = r['column']['vid']
            rec['column'] = r['column']['name']

        if 'table' in r:
            rec['id'] = r['table']['vid']
            rec['table'] = r['table']['name']

        if 'partition' in r:
            rec['id'] = r['partition']['vid']
            rec['partition'] = r['partition']['vname']


        prt(ts, **rec)

    return 

def library_schema(args, l, config):
    from databundles.bundle import DbBundle    


    # This will fetch the data, but the return values aren't quite right
    r = l.get(args.term, cb=Progressor().progress)


    abs_path = os.path.join(l.cache.cache_dir, r.identity.cache_key)
    b = DbBundle(abs_path)

    if args.format == 'csv':
        b.schema.as_csv()
    elif args.format == 'json':
        import json
        s = b.schema.as_struct()
        if args.pretty:
            print(json.dumps(s, sort_keys=True,indent=4, separators=(',', ': ')))
        else:
            print(json.dumps(s))
    elif args.format == 'yaml': 
        import yaml 
        s = b.schema.as_struct()
        if args.pretty:
            print(yaml.dump(s,indent=4, default_flow_style=False))
        else:
            print(yaml.dump(s))
    else:
        raise Exception("Unknown format" )    
        
def _print_info(l,d,p, list_partitions=False):
    from cache import RemoteMarker
    
    api = None
    try:
        api = l.remote.get_upstream(RemoteMarker)
    except AttributeError: # No api
        api = l.remote
    
    remote_d = None
    remote_p = None
    
    if api:
        from client.exceptions import NotFound
        try:
            r = api.get(d.vid, p.vid if p else None)
            if r:
                remote_d = r['dataset']
                remote_p = r['partitions'].items()[0][1] if p and 'partitions' in r and len(r['partitions']) != 0 else None
        except NotFound as e:
            pass 

    prt("D --- Dataset ---")
    prt("D Dataset   : {}; {}",d.vid, d.vname)
    prt("D Is Local  : {}",l.cache.has(d.cache_key) is not False)
    prt("D Rel Path  : {}",d.cache_key)
    prt("D Abs Path  : {}",l.cache.path(d.cache_key) if l.cache.has(d.cache_key) else '')

    if remote_d:
        prt("D Web Path  : {}",remote_d['url'])
    
    if l.cache.has(d.cache_key):
        b = l.get(d.vid)
        prt("D Partitions: {}",b.partitions.count)
        if not p and (list_partitions or b.partitions.count < 12):
            
            for partition in  b.partitions.all:
                prt("P {:15s} {}", partition.identity.vid, partition.identity.vname)
        
    else:
        print(remote_d)

    if p:
        prt("P --- Partition ---")
        prt("P Partition : {}; {}",p.vid, p.vname)
        prt("P Is Local  : {}",(l.cache.has(p.cache_key) is not False) if p else '')
        prt("P Rel Path  : {}",p.cache_key)
        prt("P Abs Path  : {}",l.cache.path(p.cache_key) if l.cache.has(p.cache_key) else '' )   
  
        if remote_p:
            prt("P Web Path  : {}",remote_p['url'])
  
def library_get(args, l, config):

    # This will fetch the data, but the return values aren't quite right
    r = l.get(args.term, force=args.force, cb=Progressor('Download {}'.format(args.term)).progress)
  
    if not r:
        prt("{}: Not found",args.term)
        return  


    _print_info(l,r.identity, r.partition.identity if r.partition else None)

    if r and args.open:
        
        if r.partition:
            abs_path = os.path.join(l.cache.cache_dir, r.partition.identity.cache_key)
        else:
            abs_path = os.path.join(l.cache.cache_dir, r.identity.cache_key)
            
        prt("\nOpening: {}\n",abs_path)

        os.execlp('sqlite3','sqlite3',abs_path )
        

def library_load(args, l, config):       

    from bundle import get_identity
    from identity import Identity
    
    
    print(Identity.parse_name(args.relpath).to_dict())
    
    return 
    
    prt("{}",l.cache.connection_info)
    prt("{}: Load relpath from cache", args.relpath)
    path = l.cache.get(args.relpath)
        
    prt("{}: Stored in local cache", path)
        
    if path:
        print(get_identity(path).name)

    
def library_unknown(args, l, config):
    err("Unknown subcommand")
    err(args)

def remote_command(args, rc, src):
    import library

    if args.is_server:
        config  = src
    else:
        config = rc
    
    l = library.new_library(config.library(args.name))

    globals()['remote_'+args.subcommand](args, l,config)



def remote_info(args, l, rc):
    from identity import new_identity
    
    if args.term:

        dsi = l.remote.get_ref(args.term)

        if not dsi:
            err("Failed to find record for: {}", args.term)
            return 

        d = new_identity(dsi['dataset'])
        p = new_identity(dsi['partitions'].items()[0][1]) if dsi['ref_type'] == 'partition' else None
                
        _print_info(l,d,p)

    else:
        prt(str(l.remote))

def remote_list(args, l, rc):
        
    if args.datasets:
        # List just the partitions in some data sets. This should probably be combined into info. 
        for ds in args.datasets:
            dsi = l.remote.get_ref(ds)

            prt("dataset {0:11s} {1}",dsi['dataset']['id'],dsi['dataset']['name'])

            for id_, p in dsi['partitions'].items():
                vs = ''
                for v in ['time','space','table','grain','format']:
                    val = p.get(v,False)
                    if val:
                        vs += "{}={} ".format(v, val)
                prt("        {0:11s} {1:50s} {2} ",id_,  p['name'], vs)
            
    else:

        datasets = l.remote.list(with_metadata=args.meta)

        for id_, data in datasets.items():
            prt("{:10s} {:50s} {:s}",data['identity']['vid'],data['identity']['vname'],id_)  


def remote_find(args, l, config):
    return _find(args, l, config, True)


def bq_command(args, rc, src):
    import library


    globals()['bq_'+args.subcommand](args,rc)


def bq_cred(args, config):

    from databundles.warehouse.bigquery import BigQuery

    bq = BigQuery(config.account('bq-server'))
    
    bq.authorize_server()

def bq_list(args, config):

    from databundles.warehouse.bigquery import BigQuery
         
    bq = BigQuery(config.account('bq-server'))

    bq.authorize_server()
    
    bq.list()

def ckan_command(args,rc, src):
    from databundles.dbexceptions import ConfigurationError
    import databundles.client.ckan
    import requests
    
    repo_name = args.name
    
    repo_group = rc.group('repository')
    if not repo_group.get(repo_name): 
        raise ConfigurationError("'repository' group in configure either nonexistent"+
                                 " or missing {} sub-group ".format(repo_name))
    
    repo_config = repo_group.get(repo_name)
    
    api = databundles.client.ckan.Ckan( repo_config.url, repo_config.key)   
    
    if args.subcommand == 'package':
        try:
            pkg = api.get_package(args.term)
        except requests.exceptions.HTTPError:
            return
        
        if args.use_json:
            import json
            prt(json.dumps(pkg, sort_keys=True, indent=4, separators=(',', ': ')))
        else:
            import yaml
            yaml.dump(args, indent=4, default_flow_style=False)

    else:
        pass
 

def source_command(args, rc, src):


    globals()['source_'+args.subcommand](args, rc,src)

def source_info(args,rc, src):
    
    if not args.term:
        prt("Source dir: {}", rc.sourcerepo.dir)
        for repo in  rc.sourcerepo.list:
            prt("Repo      : {}", repo.ident)
    else:
        import library
        from identity import new_identity
        l = library.new_library(rc.library(args.library))  
        found = False      
        for r in l.database.get_file_by_type('source'):
            ident = new_identity(r.data)
            if args.term == ident.name or args.term == ident.vname:
                found = r
                break
                
        if not found:
            err("Didn't find source for term '{}'. (Maybe need to run 'source sync')", args.term)
        else:
            from source.repository import new_repository
            repo = new_repository(rc.sourcerepo(args.name))
            ident = new_identity(r.data)
            repo.bundle_ident = ident
            
            prt('Name      : {}', ident.vname)
            prt('Id        : {}', ident.vid)
            prt('Dir       : {}', repo.bundle_dir)
            
            if not repo.bundle.database.exists():
                prt('Exists    : Database does not exist or is empty')
            else:   
                
                d = dict(repo.bundle.db_config.dict)
                process = d['process']

                prt('Created   : {}', process['dbcreated'] if process['dbcreated'] else '')
                prt('Prepared  : {}', process['prepared'] if process['prepared'] else '')
                prt('Built     : {}', process['built'] if process['built'] else '')
                prt('Build time: {} s', round(float(process['buildtime']),2) if process['buildtime'] else '')

                
             
def source_init(args,rc, src):
    from source.repository import new_repository

    dir = args.dir
    
    if not dir:
        dir = os.getcwd()
    
    repo = new_repository(rc.sourcerepo(args.name))
    repo.bundle_dir = dir

    repo.delete_remote()
    import time
    time.sleep(3)
    repo.init()
    repo.init_remote()
    
    repo.push()
    
def source_sync(args,rc, src):
    '''Synchronize all of the repositories with the local library'''
    from source.repository import new_repository
    import library
    from identity import new_identity

    l = library.new_library(rc.library(args.library))


    for repo in rc.sourcerepo.list:
        for e in repo.service.list():

            ident = new_identity(e)

            l.database.add_file(e['clone_url'], repo.service.ident, ident.name, state='synced', type_='source', data=e)
            
            prt("Added {:50s} {}",ident.name,e['clone_url'] )

  
def _source_list(dir_):
    lst = {}
    for root, dirs, files in os.walk(dir_):
        if 'bundle.yaml' in files:
            bundle_class = load_bundle(root)
            bundle = bundle_class(root)
            
            ident = bundle.identity.to_dict()
            ident['in_source'] = True
            ident['source_dir'] = root
            ident['source_built'] = True if bundle.is_built else False
            lst[ident['name']] = ident

    return lst

def _library_list(l):
    
    lst = {}
    for r in l.list():
        r['in_library'] = True
        
        lst[r['name']] = r
        
    return lst
        
         
def _print_bundle_list(*args):
    '''Create a nice display of a list of source packages'''
    from collections import defaultdict
    
    lists = []
    names = set()
    for lst in args:
        lists.append(defaultdict(dict,lst))
        names.update(lst.keys())
  
    f_lst = defaultdict(dict)
    
    for name in names:
        f_lst[name] = {}
        for lst in lists:
            f_lst[name].update(lst[name])

    for k,v in sorted(f_lst.items(), key=lambda x: x[0]):
        flags = [ 'S' if v.get('in_source', False) else ' ',
                  'B' if v.get('source_built', False) else ' ',
                  'L' if v.get('in_library', False) else ' ',
                 ]
        
        prt("{} {:35s}",''.join(flags), k, v['source_dir'])
          
def source_list(args,rc, src, names=None):
    '''List all of the source packages'''
    from collections import defaultdict
    import library
    
    dir = rc.sourcerepo.dir
    l = library.new_library(rc.library(args.library))

    if not names:
        l_lst = defaultdict(dict, _library_list(l))
        s_lst = defaultdict(dict, _source_list(dir))
    else:
        l_lst = defaultdict(dict, { k:v for k,v in _library_list(l).items() if k in names})
        s_lst = defaultdict(dict, { k:v for k,v in _source_list(dir).items() if k in names})

    _print_bundle_list(s_lst, l_lst)

            
        
def source_clone(args,rc, src):   
    '''Clone one or more registered source packages ( via sync ) into the source directory '''
    import library
    from dbexceptions import ConflictError
    from identity import new_identity
    l = library.new_library(rc.library(args.library))

    def get_by_group(group):
        return [f for f in  l.database.get_file_by_type('source') if f.group == group]

    for repo in rc.sourcerepo.list:
        print ("--- Cloning sources from: ", repo.ident)
        for f in get_by_group(repo.ident):
            try:
                ident = new_identity(f.data)
                d = repo.clone(f.path, ident.source_path,repo.dir) 
                prt("Cloned {} to {}",f.path, d)
            except ConflictError as e :
                warn("Clone failed for {}: {}".format(f.path, e.message))
                
def source_new(args,rc, src):   
    '''Clone one or more registered source packages ( via sync ) into the source directory '''
    from source.repository import new_repository
    from identity import new_identity, DatasetNumber
    
    repo = new_repository(rc.sourcerepo(args.name))  

    ident = new_identity(vars(args))

    bundle_dir =  os.path.join(repo.dir, ident.source_path)

    if not os.path.exists(bundle_dir):
        os.makedirs(bundle_dir)
    elif not os.path.isdir(bundle_dir):
        raise IOError("Directory already exists: "+bundle_dir)

    config ={'identity':{
         'id': str(DatasetNumber()),
         'source': args.source,
         'creator': args.creator,
         'dataset':args.dataset,
         'subset': args.subset,
         'variation': args.variation,
         'revision': args.revision
         }}
    
    file_ = os.path.join(bundle_dir, 'bundle.yaml')
    yaml.dump(config, file(file_, 'w'), indent=4, default_flow_style=False)

    bundle_file =  os.path.join(os.path.dirname(__file__),'support','bundle.py')

    shutil.copy(bundle_file, bundle_dir  )

    os.makedirs(os.path.join(bundle_dir, 'meta'))

    schema_file =  os.path.join(os.path.dirname(__file__),'support','schema.csv')
    
    shutil.copy(schema_file, os.path.join(bundle_dir, 'meta')  )

    prt("CREATED: {}",bundle_dir)

def load_bundle(bundle_dir):
    from databundles.run import import_file
    
    rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
    mod = import_file(rp)
  
    return mod.Bundle
    
def source_make(args,rc, src):
    from os import walk
    from os.path import basename
    from databundles.identity import Identity
    
    if args.dir:
        if os.path.exists(args.dir):
            dir = args.dir
            name = None
        else:
            try: 
                Identity.parse_name(args.dir)
                dir = None
                name = args.dir
            except:  
                err("Argument '{}' must be either a bundle name or a directory")
            
        
    if not dir:
        dir = rc.sourcerepo.dir
        
    
        
    def build(bundle_dir):
        from library import new_library
        # Import the bundle file from the directory

        bundle_class = load_bundle(bundle_dir)
        bundle = bundle_class(bundle_dir)

        l = new_library(rc.library(args.library))

    
        if l.get(bundle.identity.vid)  and not args.force:
            bundle.log("Bundle {} is already in library".format(bundle.identity.name))

        elif bundle.is_built and not args.force and not args.clean:
            bundle.log("Bundle {} is already built".format(bundle.identity.name))
        else:
            bundle.log("-------------")
            bundle.log("Building {} ".format(bundle.identity.name))
            bundle.log("-------------")

            bundle.clean()
            bundle = bundle_class(bundle_dir)
    
            if not bundle.run_prepare():
                err("Prepare failed")
            
            if not bundle.run_build():
                err("Build failed")
            

            
        if args.install:
            if not bundle.run_install(force=True):
                err('Install failed')
            
       
    if name:
        from source.repository import new_repository
        repo = new_repository(rc.sourcerepo(args.name))        

        deps = repo.bundle_deps(name)
        deps.append(name)

        build_dirs = {}
        for root, dirs, files in os.walk(dir):
            if 'bundle.yaml' in files:
                bundle_class = load_bundle(root)
                bundle = bundle_class(root)      
                build_dirs[bundle.identity.name] = root 
                
        for n in deps:
            dir_ = build_dirs[n]
            prt("{:50s} {}".format(n, dir_))
            build(dir_)
    
        
    else:
        for root, dirs, files in os.walk(dir):
            if 'bundle.yaml' in files:
                build(root)


def source_run(args,rc, src):
    from os.path import basename
    from source.repository.git import GitRepository

    print(args)
    dir = args.dir

    if not dir:
        dir = rc.sourcerepo.dir

    for root, dirs, files in os.walk(dir):
        if 'bundle.yaml' in files:
            repo = GitRepository(None, root)
            repo.bundle_dir = root
            
            if args.repo_command == 'commit' and repo.needs_commit():
                prt("--- {} {}",args.repo_command, root)
                repo.commit(''.join(args.message))
                
            elif args.repo_command == 'push' and repo.needs_push():
                prt("--- {} {}",args.repo_command, root)
                repo.push()
                
            elif args.repo_command == 'pull':
                prt("--- {} {}",args.repo_command, root)
                
            elif args.repo_command == 'install':
                prt("--- {} {}",args.repo_command, root)    
                bundle_class = load_bundle(root)
                bundle = bundle_class(root)
        
                bundle.run_install()
        
        
            elif args.shell_command:
                
                cmd = ' '.join(args.shell_command)
                
                saved_path = os.getcwd()
                os.chdir(root)   
                prt('----- {}', root)
                prt('----- {}', cmd)
        
                os.system(cmd)
                prt('')
                os.chdir(saved_path)         
       
def source_find(args,rc, src):
    from source.repository.git import GitRepository
    
    dir = args.dir
    
    if not dir:
        dir = rc.sourcerepo.dir   

    for root, dirs, files in os.walk(dir):
        if 'bundle.yaml' in files:

            repo = GitRepository(None, root)
            repo.bundle_dir = root
            if args.commit:
                if repo.needs_commit():
                    print(root)
            elif args.push:
                if repo.needs_push():
                    print(root)
            else:
                err("Must specify either --push or --commit")
                
                
            
            
                     
def test_command(args,rc, src):
    
    if args.subcommand == 'config':
        prt(rc.dump())
    elif args.subcommand == 'spatialite':
        from pysqlite2 import dbapi2 as db
        import os
        
        f = '/tmp/_db_spatialite_test.db'
        
        if os.path.exists(f):
            os.remove(f)
        
        conn = db.connect(f)
    
        cur = conn.cursor()
        
        try:
            conn.enable_load_extension(True)
            conn.execute("select load_extension('/usr/lib/libspatialite.so')")
            loaded_extension = True
        except AttributeError:
            loaded_extension = False
            prt("WARNING: Could not enable load_extension(). ")
        
        rs = cur.execute('SELECT sqlite_version(), spatialite_version()')

        for row in rs:
            msg = "> SQLite v%s Spatialite v%s" % (row[0], row[1])
            print(msg)

    
    else:
        prt('Testing')
        prt(args)

def source_deps(args,rc, src):
    """Produce a list of dependencies for all of the source bundles"""

    from util import toposort
    from source.repository import new_repository
    from databundles.identity import Identity

    repo = new_repository(rc.sourcerepo(args.name))        


    if args.ref:

        if args.direction == 'f':
            deps = repo.bundle_deps(args.ref)
            
            if args.detail:
                source_list(args,rc, src, names=deps)    
            else:
                for b in deps:
                    prt(b)                   
            
        else:
            deps = repo.bundle_deps(args.ref, reverse=True)

            if args.detail:
                source_list(args,rc, src, names=deps)    
            else:
                for b in deps:
                    prt(b)    

        
    else:
        import pprint
        graph = toposort(repo.dependencies)
    
        for v in graph:
            pprint.pprint(v)
            
        
def main():
    import argparse
    
    parser = argparse.ArgumentParser(prog='python -mdatabundles',
                                     description='Databundles {}. Management interface for databundles, libraries and repositories. '.format(__version__))
    
    #parser.add_argument('command', nargs=1, help='Create a new bundle') 
 
    parser.add_argument('-c','--config', default=None, action='append', help="Path to a run config file") 
    parser.add_argument('-v','--verbose', default=None, action='append', help="Be verbose") 
    parser.add_argument('--single-config', default=False,action="store_true", help="Load only the config file specified")

  
    cmd = parser.add_subparsers(title='commands', help='command help')
    


    #
    # Library Command
    #
    lib_p = cmd.add_parser('library', help='Manage a library')
    lib_p.set_defaults(command='library')
    asp = lib_p.add_subparsers(title='library commands', help='command help')
    lib_p.add_argument('-n','--name',  default='default',  help='Select a different name for the library')
        
    group = lib_p.add_mutually_exclusive_group()
    group.add_argument('-s', '--server',  default=False, dest='is_server',  action='store_true', help = 'Select the server configuration')
    group.add_argument('-c', '--client',  default=False, dest='is_server',  action='store_false', help = 'Select the client configuration')
        
        
    sp = asp.add_parser('push', help='Push new library files')
    sp.set_defaults(subcommand='push')
    sp.add_argument('-w','--watch',  default=False,action="store_true",  help='Check periodically for new files.')
    sp.add_argument('-f','--force',  default=False,action="store_true",  help='Push all files')
    
    sp = asp.add_parser('server', help='Run the library server')
    sp.set_defaults(subcommand='server') 
    sp.add_argument('-d','--daemonize', default=False, action="store_true",   help="Run as a daemon") 
    sp.add_argument('-k','--kill', default=False, action="store_true",   help="With --daemonize, kill the running daemon process") 
    sp.add_argument('-g','--group', default=None,   help="Set group for daemon operation") 
    sp.add_argument('-u','--user', default=None,  help="Set user for daemon operation")  
    sp.add_argument('-t','--test', default=False, action="store_true",   help="Run the test version of the server")   
      
    sp = asp.add_parser('files', help='Print out files in the library')
    sp.set_defaults(subcommand='files')
    sp.add_argument('-a','--all',  default='all',action="store_const", const='all', dest='file_state',  help='Print all files')
    sp.add_argument('-n','--new',  default=False,action="store_const", const='new',  dest='file_state', help='Print new files')
    sp.add_argument('-p','--pushed',  default=False,action="store_const", const='pushed', dest='file_state',  help='Print pushed files')
    sp.add_argument('-u','--pulled',  default=False,action="store_const", const='pulled', dest='file_state',  help='Print pulled files')
 
    sp = asp.add_parser('new', help='Create a new library')
    sp.set_defaults(subcommand='new')
    
    sp = asp.add_parser('drop', help='Delete all of the tables in the library')
    sp.set_defaults(subcommand='drop')    
    
    sp = asp.add_parser('clean', help='Remove all entries from the library database')
    sp.set_defaults(subcommand='clean')
    
    sp = asp.add_parser('purge', help='Remove all entries from the library database and delete all files')
    sp.set_defaults(subcommand='purge')
    
    sp = asp.add_parser('list', help='List datasets in the library, or partitions in dataset')
    sp.set_defaults(subcommand='list')
    sp.add_argument('term', type=str, nargs='?', help='Name of bundle, to list partitions')
    
    sp = asp.add_parser('rebuild', help='Rebuild the library database from the files in the library')
    sp.set_defaults(subcommand='rebuild')
    sp.add_argument('-r','--remote',  default=False, action="store_true",   help='Rebuild from teh remote')
    
    sp = asp.add_parser('backup', help='Backup the library database to the remote')
    sp.set_defaults(subcommand='backup')
    sp.add_argument('-f','--file',  default=None,   help="Name of file to back up to") 
    sp.add_argument('-d','--date',  default=False, action="store_true",   help='Append the date and time, in ISO format, to the name of the file ')
    sp.add_argument('-r','--remote',  default=False, action="store_true",   help='Also load store file to  configured remote')
    sp.add_argument('-c','--cache',  default=False, action="store_true",   help='Also load store file to  configured cache')

    sp = asp.add_parser('restore', help='Restore the library database from the remote')
    sp.set_defaults(subcommand='restore')
    sp.add_argument('-f','--file',  default=None,   help="Base pattern of file to restore from.") 
    sp.add_argument('-d','--dir',  default=None,   help="Directory where backup files are stored. Will retrieve the most recent. ") 
    sp.add_argument('-r','--remote',  default=False, action="store_true",   help='Also load file from configured remote')
    sp.add_argument('-c','--cache',  default=False, action="store_true",   help='Also load file from configured cache')
 
    sp = asp.add_parser('info', help='Display information about the library or a bundle or partition')
    sp.set_defaults(subcommand='info')   
    sp.add_argument('term',  nargs='?', type=str,help='Name or ID of the bundle or partition to print information for')
    
    sp = asp.add_parser('get', help='Search for the argument as a bundle or partition name or id. Possible download the file from the remote library')
    sp.set_defaults(subcommand='get')   
    sp.add_argument('term', type=str,help='Query term')
    sp.add_argument('-o','--open',  default=False, action="store_true",  help='Open the database with sqlite')
    sp.add_argument('-f','--force',  default=False, action="store_true",  help='Force retrieving from the remote')

    sp = asp.add_parser('delete', help='Delete a file from all local caches and the local library')
    sp.set_defaults(subcommand='delete')
    sp.add_argument('term', type=str,help='Name or ID of the bundle or partition to remove')

    sp = asp.add_parser('load', help='Search for the argument as a bundle or partition name or id. Possible download the file from the remote library')
    sp.set_defaults(subcommand='load')   
    sp.add_argument('relpath', type=str,help='Cache rel path of dataset to load from remote')


    sp = asp.add_parser('find', help='Search for the argument as a bundle or partition name or id')
    sp.set_defaults(subcommand='find')   
    sp.add_argument('term', type=str, nargs=argparse.REMAINDER,help='Query term')


    sp = asp.add_parser('schema', help='Dump the schema for a bundle')
    sp.set_defaults(subcommand='schema')   
    sp.add_argument('term', type=str,help='Query term')
    sp.add_argument('-p','--pretty',  default=False, action="store_true",  help='pretty, formatted output')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-y', '--yaml',  default='csv', dest='format',  action='store_const', const='yaml')
    group.add_argument('-j', '--json',  default='csv', dest='format',  action='store_const', const='json')
    group.add_argument('-c', '--csv',  default='csv', dest='format',  action='store_const', const='csv')

    #
    # warehouse  Command
    #
    
    whr_p = cmd.add_parser('warehouse', help='Manage a warehouse')
    whr_p.set_defaults(command='warehouse')
    whp = whr_p.add_subparsers(title='warehouse commands', help='command help')
 
    group = whr_p.add_mutually_exclusive_group()
    group.add_argument('-s', '--server',  default=False, dest='is_server',  action='store_true', help = 'Select the server configuration')
    group.add_argument('-c', '--client',  default=False, dest='is_server',  action='store_false', help = 'Select the client configuration')
        
    whr_p.add_argument('-l','--library',  default='default',  help='Select a different name for the library')
    whr_p.add_argument('-n','--name',  default='default',  help='Select a different name for the warehouse')

    whsp = whp.add_parser('install', help='Install a bundle or partition to a warehouse')
    whsp.set_defaults(subcommand='install')
    whsp.add_argument('term', type=str,help='Name of bundle or partition')

    whsp = whp.add_parser('remove', help='Remove a bundle or partition from a warehouse')
    whsp.set_defaults(subcommand='remove')
    whsp.add_argument('term', type=str,help='Name of bundle or partition')
    
    
    whsp = whp.add_parser('sync', help='Syncronize database to a list of names')
    whsp.set_defaults(subcommand='sync')
    whsp.add_argument('file', type=str,help='Name of file containing a list of names')
    
    whsp = whp.add_parser('connect', help='Test connection to a warehouse')
    whsp.set_defaults(subcommand='connect')

    whsp = whp.add_parser('info', help='Configuration information')
    whsp.set_defaults(subcommand='info')   
 
    whsp = whp.add_parser('drop', help='Drop the warehouse database')
    whsp.set_defaults(subcommand='drop')   
 
    #
    # ckan Command
    #
    lib_p = cmd.add_parser('ckan', help='Access a CKAN repository')
    lib_p.set_defaults(command='ckan')
    lib_p.add_argument('-n','--name',  default='default',  help='Select the configuration name for the repository')
    asp = lib_p.add_subparsers(title='CKAN commands', help='Access a CKAN repository')
    
    sp = asp.add_parser('package', help='Dump a package by name, as json or yaml')
    sp.set_defaults(subcommand='package')   
    sp.add_argument('term', type=str,help='Query term')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-y', '--yaml',  default=True, dest='use_json',  action='store_false')
    group.add_argument('-j', '--json',  default=True, dest='use_json',  action='store_true')
    
    #
    # Install Command
    #
    lib_p = cmd.add_parser('install', help='Install configuration files')
    lib_p.set_defaults(command='install')
    asp = lib_p.add_subparsers(title='Install', help='Install configuration files')
    
    #
    # Config Command
    #
    sp = asp.add_parser('config', help='Install the global configuration')
    sp.set_defaults(subcommand='config')
    sp.add_argument('-p', '--print',  dest='prt', default=False, action='store_true', help='Print, rather than save, the config file')
    sp.add_argument('-f', '--force',  default=False, action='store_true', help="Force using the default config; don't re-use the xisting config")
    sp.add_argument('-r', '--root',  default=None,  help="Set the root dir")
    sp.add_argument('-R', '--remote',  default=None,  help="Url of remote library")


    #
    # Source Command
    #
    src_p = cmd.add_parser('source', help='Manage bundle source files')
    src_p.set_defaults(command='source')
    src_p.add_argument('-n','--name',  default='default',  help='Select the name for the repository. Defaults to "default" ')
    src_p.add_argument('-l','--library',  default='default',  help='Select a different name for the library')
    asp = src_p.add_subparsers(title='source commands', help='command help')  
   

    sp = asp.add_parser('new', help='Create a new bundle')
    sp.set_defaults(subcommand='new')
    sp.set_defaults(revision=1) # Needed in Identity.name_parts
    sp.add_argument('-s','--source', required=True, help='Source, usually a domain name') 
    sp.add_argument('-d','--dataset',  required=True, help='Name of the dataset') 
    sp.add_argument('-b','--subset', nargs='?', default=None, help='Name of the subset') 
    sp.add_argument('-v','--variation', default='orig', help='Name of the variation') 
    sp.add_argument('-c','--creator',  required=True, help='Id of the creator') 
    sp.add_argument('-n','--dry-run', default=False, help='Dry run') 
    sp.add_argument('args', nargs=argparse.REMAINDER) # Get everything else. 

    sp = asp.add_parser('info', help='Information about the source configuration')
    sp.set_defaults(subcommand='info')
    sp.add_argument('term',  nargs='?', type=str,help='Name or ID of the bundle or partition to print information for')
    
    
    sp = asp.add_parser('deps', help='Print the depenencies for all source bundles')
    sp.set_defaults(subcommand='deps')
    sp.add_argument('ref', type=str,nargs='?',help='Name or id of a bundle to generate a sorted dependency list for.')   
    sp.add_argument('-d','--detail',  default=False,action="store_true",  help='Display details of locations for each bundle')   
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-f', '--forward',  default='f', dest='direction',   action='store_const', const='f', help='Display bundles that this one depends on')
    group.add_argument('-r', '--reverse',  default='f', dest='direction',   action='store_const', const='r', help='Display bundles that depend on this one')
    
    sp = asp.add_parser('find', help='Find source bundle source directories')
    sp.set_defaults(subcommand='find')
    sp.add_argument('term', type=str,help='Query term')
    sp.add_argument('-r','--register',  default=False,action="store_true",  help='Register directories in the library. ')

    sp = asp.add_parser('init', help='Intialize the local and remote git repositories')
    sp.set_defaults(subcommand='init')
    sp.add_argument('dir', type=str,nargs='?',help='Directory')

    sp = asp.add_parser('list', help='List the source dirctories')
    sp.set_defaults(subcommand='list')

    sp = asp.add_parser('sync', help='Load references from the confiurged source remotes')
    sp.set_defaults(subcommand='sync')
    sp.add_argument('-l','--library',  default='default',  help='Select a library to add the references to')
  
    sp = asp.add_parser('clone', help='Clone source into a local directory')
    sp.set_defaults(subcommand='clone')
    sp.add_argument('-l','--library',  default='default',  help='Select a library to take references from')
    sp.add_argument('dir', type=str,nargs='?',help='Source id')      
  
    sp = asp.add_parser('make', help='Build sources')
    sp.set_defaults(subcommand='make')
    sp.add_argument('-f','--force', default=False,action="store_true", help='Build even if built or in library')
    sp.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    sp.add_argument('-i','--install', default=False,action="store_true", help='Install after build')

    sp.add_argument('dir', type=str,nargs='?',help='Directory to start search for sources in. ')      
 
 
    sp = asp.add_parser('run', help='Run a shell command in source directories')
    sp.set_defaults(subcommand='run')
    sp.add_argument('-d','--dir', nargs='?', help='Directory to start recursing from ')
    sp.add_argument('-m','--message', nargs='+', default='.', help='Directory to start recursing from ')
    sp.add_argument('shell_command',nargs=argparse.REMAINDER, type=str,help='Shell command to run')  
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-c', '--commit',  default=False, dest='repo_command',   action='store_const', const='commit', help='Commit')
    group.add_argument('-p', '--push',  default=False, dest='repo_command',   action='store_const', const='push', help='Push to origin/master')    
    group.add_argument('-l', '--pull',  default=False, dest='repo_command',   action='store_const', const='pull', help='Pull from upstream')  
    group.add_argument('-i', '--install',  default=False, dest='repo_command',   action='store_const', const='install', help='Install the bundle')  
      
            
      
    sp = asp.add_parser('find', help='Find source packages that meet a vareity of conditions')
    sp.set_defaults(subcommand='find')
    sp.add_argument('-d','--dir',  help='Directory to start recursing from ')
    group = sp.add_mutually_exclusive_group()
    group.add_argument('-c', '--commit',  default=False, dest='commit',   action='store_true', help='Find bundles that need to be committed')
    group.add_argument('-p', '--push',  default=False, dest='push',   action='store_true', help='Find bundles that need to be pushed')
      
    #
    # Remote Command
    #
    
    lib_p = cmd.add_parser('remote', help='Access the remote library')
    lib_p.set_defaults(command='remote')
    asp = lib_p.add_subparsers(title='remote commands', help='Access the remote library')
    lib_p.add_argument('-n','--name',  default='default',  help='Select a different name for the library, from which the remote is located')
 
    group = lib_p.add_mutually_exclusive_group()
    group.add_argument('-s', '--server',  default=False, dest='is_server',  action='store_true', help = 'Select the server configuration')
    group.add_argument('-c', '--client',  default=False, dest='is_server',  action='store_false', help = 'Select the client configuration')
        
    sp = asp.add_parser('info', help='Display the remote configuration')
    sp.set_defaults(subcommand='info')
    sp.add_argument('term',  nargs='?', type=str,help='Name or ID of the bundle or partition to print information for')
    
  
    sp = asp.add_parser('list', help='List remote files')
    sp.set_defaults(subcommand='list')
    sp.add_argument('-m','--meta', default=False,  action='store_true',  help="Force fetching metadata for remotes that don't provide it while listing, like S3")
    sp.add_argument('datasets', nargs=argparse.REMAINDER)
        
    sp = asp.add_parser('find', help='Search for the argument as a bundle or partition name or id')
    sp.set_defaults(subcommand='find')   
    sp.add_argument('term', type=str, nargs=argparse.REMAINDER,help='Query term')


    #
    # BigQuery
    #
    lib_p = cmd.add_parser('bq', help='BigQuery administration')
    lib_p.set_defaults(command='bq')
    asp = lib_p.add_subparsers(title='Bigquerry Commands', help='command help')
    
    sp = asp.add_parser('cred', help='Setup access credentials')
    sp.set_defaults(subcommand='cred')
    
    sp = asp.add_parser('list', help='List datasets')
    sp.set_defaults(subcommand='list')
          
    #
    # Test Command
    #
    lib_p = cmd.add_parser('test', help='Test and debugging')
    lib_p.set_defaults(command='test')
    asp = lib_p.add_subparsers(title='Test commands', help='command help')
    
    sp = asp.add_parser('config', help='Dump the configuration')
    sp.set_defaults(subcommand='config')
    group.add_argument('-v', '--version',  default=False, action='store_true', help='Display module version')
 
    sp = asp.add_parser('spatialite', help='Test spatialite configuration')
    sp.set_defaults(subcommand='spatialite')
         
    args = parser.parse_args()

    if args.single_config:
        if args.config is None or len(args.config) > 1:
            raise Exception("--single_config can only be specified with one -c")
        else:
            rc_path = args.config
    elif args.config is not None and len(args.config) == 1:
            rc_path = args.config.pop()
    else:
        rc_path = args.config
        
   
    funcs = {
        'library':library_command,
        'warehouse':warehouse_command,
        'remote':remote_command,
        'test':test_command,
        'install':install_command,
        'ckan':ckan_command,
        'source': source_command,
        'bq': bq_command
    }
        
    f = funcs.get(args.command, False)
        
    if f != install_command:
        rc = get_runconfig(rc_path)
        src = get_runconfig(rc_path, is_server = True)
    else:
        rc = None
        src = None
        
    if not f:
        err("Error: No command: "+args.command)
    else:
        try:
            f(args, rc, src)
        except KeyboardInterrupt:
            prt('\nExiting...')
            pass
        

       



if __name__ == '__main__':
    main()