"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import prt, err, _print_info, _find #@UnresolvedImport

def remote_command(args, rc, src):
    from databundles.library import new_library

    if args.is_server:
        config  = src
    else:
        config = rc
    
    l = new_library(config.library(args.name))

    globals()['remote_'+args.subcommand](args, l,config)



def remote_info(args, l, rc):
    from ..identity import new_identity
    from databundles.client.exceptions import NotFound
    
    if args.term:
        try:
            dsi = l.remote.get_ref(args.term)
        except NotFound:
            dsi = None

        if not dsi:
            err("Failed to find record for: {}", args.term)
            return 

        d = new_identity(dsi['dataset'])
        p = new_identity(dsi['partitions'].items()[0][1]) if dsi['ref_type'] == 'partition' else None
                
        _print_info(l,d,p)

    else:
        prt(str(l.remote))

def remote_list(args, l, rc, return_meta=False):
        
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

        datasets = l.remote.list(with_metadata=return_meta)


        for id_, data in sorted(datasets.items(), key = lambda x: x[1]['identity']['vname']):

            try:
                prt("{:10s} {:50s} {:s}",data['identity']['vid'],data['identity']['vname'],id_)  
            except Exception as e:
                prt("{:10s} {:50s} {:s}",'[ERROR]','',id_)  


def remote_find(args, l, config):
    return _find(args, l, config, True)

