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
            
            ident = bundle.identity.dict
            ident['in_source'] = True
            ident['source_dir'] = root
            ident['source_built'] = True if bundle.is_built else False
            ident['source_version'] = ident['revision']
            lst[ident['name']] = ident

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
            prt("{} {:35s}",''.join(flags), k, v.get('source_dir','<none'))

       
def _print_info(l,d,p, list_partitions=False):
    from ..cache import RemoteMarker
    from ..bundle import LibraryDbBundle # Get the bundle from the library
    from sqlalchemy.orm.exc import NoResultFound
    import time
   
    api = None
    try:
        api = l.upstream.get_upstream(RemoteMarker)
    except AttributeError: # No api
        api = l.upstream
    
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
            
            for partition in  b.partitions.all_nocsv:
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
    
    parser = argparse.ArgumentParser(prog='python -mdatabundles',
                                     description='Databundles {}. Management interface for databundles, libraries and repositories. '.format(__version__))
       
    parser.add_argument('-c','--config', default=None, action='append', help="Path to a run config file") 
    parser.add_argument('-v','--verbose', default=None, action='append', help="Be verbose") 
    parser.add_argument('--single-config', default=False,action="store_true", help="Load only the config file specified")

    cmd = parser.add_subparsers(title='commands', help='command help')
    
    from .library import library_parser, library_command
    from .warehouse import warehouse_command, warehouse_parser
    from .remote import remote_parser,remote_command
    from test import test_parser, test_command
    from install import install_parser, install_command
    from ckan import ckan_parser, ckan_command
    from source import source_command, source_parser
    from bundle import bundle_command, bundle_parser

    library_parser(cmd)  
    warehouse_parser(cmd)
    ckan_parser(cmd)
    install_parser(cmd)
    source_parser(cmd)
    remote_parser(cmd)
    test_parser(cmd)
    bundle_parser(cmd)

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
        'bundle':bundle_command,
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
        