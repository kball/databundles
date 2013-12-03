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
from databundles.util import Progressor
from databundles import __version__
import logging
from ..util import get_logger
     
logger = None # Set in main

def prt(template, *args, **kwargs):
    global logger

    logger.info(template.format(*args, **kwargs))


def err(template, *args, **kwargs):
    import sys
    global logger
    
    logger.error(template.format(*args, **kwargs))
    sys.exit(1)

def warn(template, *args, **kwargs):
    import sys
    global command
    global subcommand
    
    logger.warning(template.format(*args, **kwargs))


def load_bundle(bundle_dir):
    from databundles.run import import_file
    
    rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
    mod = import_file(rp)
  
    return mod.Bundle


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


def _source_list(dir_):
    lst = {}
    for root, _, files in os.walk(dir_):
        if 'bundle.yaml' in files:
            bundle_class = load_bundle(root)
            bundle = bundle_class(root)
            
            ident = bundle.identity.to_dict()
            ident['in_source'] = True
            ident['source_dir'] = root
            ident['source_built'] = True if bundle.is_built else False
            ident['source_version'] = ident['revision']
            lst[ident['name']] = ident

    return lst
             
    
def _library_list(l):
    
    lst = {}
    for r in l.list():
        r['in_library'] = 'L' in r['location']
        r['in_remote'] = 'R' in r['location']
        lst[r['name']] = r
        
    return lst
        
         
def _print_bundle_list(*args,**kwargs):
    '''Create a nice display of a list of source packages'''
    from collections import defaultdict
    
    subset_names = kwargs.get('subset_names', None)
    
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

    def rev_flag(v, flag):
        
        flag_map = {'L':'library', 'R':'remote'}
        
        suffix = flag_map[flag]
        
        loc = v.get('in_'+suffix, False)
        
        if not loc:
            return '  '
        
        if not v.get('in_source', False):
            return flag+' '
        
        s_rev = int(v.get('source_version'))
    
        rdiff = int(v.get(suffix+'_version')) - s_rev
        
        if rdiff > 0:
            vf = '+'
        elif rdiff < 0:
            vf = '-'
        else: 
            vf = '='
        
        return flag+vf

    for k,v in sorted(f_lst.items(), key=lambda x: x[0]):
        flags = [ 'S' if v.get('in_source', False) else ' ',
                  'B' if v.get('source_built', False) else ' ',
                  rev_flag(v,'L'),
                  rev_flag(v,'R')
                 ]
        
        if subset_names is None or k in subset_names: 
            prt("{} {:35s}",''.join(flags), k, v['source_dir'])

       
def _print_info(l,d,p, list_partitions=False):
    from ..cache import RemoteMarker
    from ..bundle import LibraryDbBundle # Get the bundle from the library
    from sqlalchemy.orm.exc import NoResultFound
    
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
        b = LibraryDbBundle(l.database, d.vid)
        
        prt("D Partitions: {}",b.partitions.count)
        if not p and (list_partitions or b.partitions.count < 12):
            
            for partition in  b.partitions.all:
                prt("P {:15s} {}", partition.identity.vid, partition.identity.vname)

    if p:
        prt("P --- Partition ---")
        prt("P Partition : {}; {}",p.vid, p.vname)
        prt("P Is Local  : {}",(l.cache.has(p.cache_key) is not False) if p else '')
        prt("P Rel Path  : {}",p.cache_key)
        prt("P Abs Path  : {}",l.cache.path(p.cache_key) if l.cache.has(p.cache_key) else '' )   

        if remote_p:
            prt("P Web Path  : {}",remote_p['urls']['db'])
            


def main():
    import argparse
    from .library import library_command #@UnresolvedImport
    from .warehouse import warehouse_command, warehouse_parser #@UnresolvedImport
    from .remote import remote_command  #@UnresolvedImport 
    from .library import library_parser
    from test import test_command #@UnresolvedImport
    from install import install_command #@UnresolvedImport
    from ckan import ckan_command #@UnresolvedImport
    from source import source_command, source_parser  #@UnresolvedImport    
    parser = argparse.ArgumentParser(prog='python -mdatabundles',
                                     description='Databundles {}. Management interface for databundles, libraries and repositories. '.format(__version__))
    
    #parser.add_argument('command', nargs=1, help='Create a new bundle') 
 
    parser.add_argument('-c','--config', default=None, action='append', help="Path to a run config file") 
    parser.add_argument('-v','--verbose', default=None, action='append', help="Be verbose") 
    parser.add_argument('--single-config', default=False,action="store_true", help="Load only the config file specified")

  
    cmd = parser.add_subparsers(title='commands', help='command help')
    

    #
    # library  Command
    #

    library_parser(cmd)

    #
    # warehouse  Command
    #
    
    warehouse_parser(cmd)
 
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


    source_parser(cmd)
   
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
    }
        
    f = funcs.get(args.command, False)
        
    if f != install_command:
        rc = get_runconfig(rc_path)
        src = get_runconfig(rc_path, is_server = True)
    else:
        rc = None
        src = None
        
    global logger

    logger = get_logger("{}.{}".format(args.command,args.subcommand  ))
    logger.setLevel(logging.INFO) 


    if not f:
        err("Error: No command: "+args.command)
    else:
        try:
            f(args, rc, src)
        except KeyboardInterrupt:
            prt('\nExiting...')
            pass
        