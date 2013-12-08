"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import prt, err, _print_info #@UnresolvedImport
from databundles.warehouse import ResolverInterface

def warehouse_command(args, rc, src):
    from databundles.warehouse import new_warehouse

    if args.is_server:
        config  = src
    else:
        config = rc

    w = new_warehouse(config.warehouse(args.name))

    globals()['warehouse_'+args.subcommand](args, w,config)

def warehouse_parser(cmd):
   
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
 
    whsp = whp.add_parser('create', help='Create required tables')
    whsp.set_defaults(subcommand='create')   
 
    whsp = whp.add_parser('list', help='List the datasets inthe warehouse')
    whsp.set_defaults(subcommand='list')   
    whsp.add_argument('term', type=str, nargs='?', help='Name of bundle, to list partitions')

   
def warehouse_info(args, w,config):
    
    prt("Warehouse Info")
    prt("Name:     {}",args.name)
    prt("Class:    {}",w.__class__)
    prt("Database: {}",w.database.dsn)
    prt("Library : {}",w.library.database.dsn)

 
class Logger(object):
    def __init__(self, prefix, lr):
        self.prefix = prefix
        self.lr = lr
        
    def progress(self,type_,name, n, message=None):
        self.lr("{}: {} {}: {}".format(self.prefix,type_, name, n))
        
    def log(self,message):
        prt("{}: {}",self.prefix, message)
        
    def error(self,message):
        err("{}: {}",self.prefix, message)
   
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

        import pprint
        
        pprint.pprint(dsi)

        if dsi['ref_type'] == 'partition':
            # For a partition reference, we get back a dataset structure, which could
            # have many partitions, but if we asked for a parttion, will get only one. 

            return dsi['partitions'].values()[0]['urls']['db']
        else:
            return dsi['dataset']['url']

    def csv_parts(self, name):
        
        dsi = self.library.remote.get_ref(name)

        if not dsi:
            return None

        if dsi['ref_type'] == 'partition':
            # For a partition reference, we get back a dataset structure, which could
            # have many partitions, but if we asked for a parttion, will get only one. 

            parts_url =  dsi['partitions'].values()[0]['urls']['csv']['parts']
            
            import requests
            
            r = requests.get(parts_url)

            return r.json()
            
        else:
            from ..dbexceptions import BadRequest
            raise BadRequest("Didn't get any csvparts")
    
def warehouse_install(args, w,config):
    from ..library import new_library
    from functools import partial
    from databundles.util import init_log_rate
    
    if not w.exists():
        w.create()

    l = new_library(config.library(args.library))
    w.resolver = Resolver(l)
    w.logger = Logger('Warehouse Install',init_log_rate(2000))
 
    w.install_by_name(args.term )

def warehouse_remove(args, w,config):
    from functools import partial
    from databundles.util import init_log_rate

    w.logger = Logger('Warehouse Remove',init_log_rate(2000))
    
    w.remove_by_name(args.term )
      
def warehouse_drop(args, w,config):
    
    w.database.enable_delete = True
    w.library.clean()
    w.drop()
 
def warehouse_create(args, w,config):
    
    w.database.enable_delete = True
    w.library.clean()
    w.drop()
    
    w.library.database.create()
    
    
def warehouse_list(args, w, config):    

    l = w.library

    if not args.term:

        for ident in sorted(l.list(), key=lambda x: x['vname']):
            prt("{:2s} {:10s} {}", ''.join(ident['location']), ident['vid'], ident['vname'])
    else:
        d, p = l.get_ref(args.term)
                
        _print_info(l,d,p, list_partitions=True)
  
