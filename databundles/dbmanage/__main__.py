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
            


def load_bundle(bundle_dir):
    from databundles.run import import_file
    
    rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
    mod = import_file(rp)
  
    return mod.Bundle

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
        
        prt("{} {:35s}",''.join(flags), k, v['source_dir'])

    
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
    sp.add_argument('-s','--synced',  default=False,action="store_const", const='synced', dest='file_state',  help='Print synced source packages')
  
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
        
    from .library import library_command #@UnresolvedImport
    from .warehouse import warehouse_command #@UnresolvedImport
    from .remote import remote_command  #@UnresolvedImport 
    from test import test_command #@UnresolvedImport
    from install import install_command #@UnresolvedImport
    from ckan import ckan_command #@UnresolvedImport
    from source import source_command #@UnresolvedImport
    from bq import bq_command #@UnresolvedImport
   
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
 