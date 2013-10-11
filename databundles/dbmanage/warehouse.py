"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbmanage import prt


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
    w.library.clean()
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
