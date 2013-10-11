"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbmanage import prt, err, Progressor
import os


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
        for f in files_:
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
        from ..client.exceptions import NotFound
        try:
            r = api.get(d.vid, p.vid if p else None)
            if r:
                remote_d = r['dataset']
                remote_p = r['partitions'].items()[0][1] if p and 'partitions' in r and len(r['partitions']) != 0 else None
        except NotFound:
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
