"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""



from ..dbmanage import prt, err, warn
from ..dbmanage import _library_list, _source_list, load_bundle, _print_bundle_list

import os
import yaml
import shutil

def source_command(args, rc, src):

    globals()['source_'+args.subcommand](args, rc,src)

def source_parser(cmd):
    import argparse
    
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
    sp.add_argument('-n','--dryrun', default=False, help='Dry run') 
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
  
    sp = asp.add_parser('build', help='Build sources')
    sp.set_defaults(subcommand='build')
    sp.add_argument('-p','--pull', default=False,action="store_true", help='Git pull before build')
    sp.add_argument('-s','--stash', default=False,action="store_true", help='Git stash before build')
    sp.add_argument('-f','--force', default=False,action="store_true", help='Build even if built or in library')
    sp.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
    sp.add_argument('-i','--install', default=False,action="store_true", help='Install after build')
    sp.add_argument('-n','--dryrun', default=False,action="store_true", help='Only display what would be built')

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
    group.add_argument('-i', '--init',  default=False, dest='init',   action='store_true', help='Find bundles that need to be initialized')
               

def source_info(args,rc, src):
    
    if not args.term:
        prt("Source dir: {}", rc.sourcerepo.dir)
        for repo in  rc.sourcerepo.list:
            prt("Repo      : {}", repo.ident)
    else:
        import databundles.library as library
        from ..identity import new_identity
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
            from ..source.repository import new_repository
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

                prt('Created   : {}', process.get('dbcreated',''))
                prt('Prepared  : {}', process.get('prepared',''))
                prt('Built     : {}', process.get('built',''))
                prt('Build time: {}', str(round(float(process['buildtime']),2))+'s' if process.get('buildtime',False) else '')

                
   
         
def source_list(args,rc, src, names=None):
    '''List all of the source packages'''
    from collections import defaultdict
    import databundles.library as library
    
    dir_ = rc.sourcerepo.dir
    l = library.new_library(rc.library(args.library))

    if not names:
        l_lst = defaultdict(dict, _library_list(l))
        s_lst = defaultdict(dict, _source_list(dir_))
    else:
        l_lst = defaultdict(dict, { k:v for k,v in _library_list(l).items() if k in names})
        s_lst = defaultdict(dict, { k:v for k,v in _source_list(dir).items() if k in names})

    _print_bundle_list(s_lst, l_lst)

            
def source_clone(args,rc, src):   
    '''Clone one or more registered source packages ( via sync ) into the source directory '''
    import databundles.library as library
    from ..dbexceptions import ConflictError
    from ..identity import new_identity
    l = library.new_library(rc.library(args.library))

    

    def get_by_group(group):
        return [f for f in  l.database.get_file_by_type('source') if f.group == group]

    for repo in rc.sourcerepo.list:
        prt ("--- Cloning sources from: {}", repo.ident)
        for f in get_by_group(repo.ident):
            try:
                ident = new_identity(f.data)
                d = repo.clone(f.path, ident.source_path,repo.dir) 
                prt("Cloned {} to {}",f.path, d)
            except ConflictError as e :
                warn("Clone failed for {}: {}".format(f.path, e.message))
                
def source_new(args,rc, src):   
    '''Clone one or more registered source packages ( via sync ) into the source directory '''
    from ..source.repository import new_repository
    from ..identity import new_identity, DatasetNumber
    
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

    bundle_file =  os.path.join(os.path.dirname(__file__),'..','support','bundle.py')

    shutil.copy(bundle_file, bundle_dir  )

    os.makedirs(os.path.join(bundle_dir, 'meta'))

    schema_file =  os.path.join(os.path.dirname(__file__),'..','support','schema.csv')
    
    shutil.copy(schema_file, os.path.join(bundle_dir, 'meta')  )

    prt("CREATED: {}",bundle_dir)


    
def source_build(args,rc, src):
    '''Build a single bundle, or a set of bundles in a directory. The build process
    will build all dependencies for each bundle before buildng the bundle. '''
    
    
    from databundles.identity import Identity
    from ..source.repository import new_repository
    
    repo = new_repository(rc.sourcerepo(args.name))   
       
    dir_ = None
    name = None
    
    if args.dir:
        if os.path.exists(args.dir):
            dir_ = args.dir
            name = None
        else:
            name = args.dir
            try: 
                Identity.parse_name(name)
            except:  
                err("Argument '{}' must be either a bundle name or a directory")
                return
            
    if not dir_:
        dir_ = rc.sourcerepo.dir
        
    
        
    def build(bundle_dir):
        from databundles.library import new_library
        from databundles.source.repository.git import GitShellService
        
        # Stash must happen before pull, and pull must happen
        # before the class is loaded in load_bundle, otherwize the class
        # can't be updated by the pull. And, we have to use the GitShell
        # sevice directly, because thenew_repository route will ooad the bundle
        
        gss = GitShellService(bundle_dir)
        
        if args.stash:
            prt("{} Stashing ", bundle_dir)
            gss.stash()
            
        if args.pull:
            prt("{} Pulling ", bundle_dir)
            gss.pull()

        # Import the bundle file from the directory

        bundle_class = load_bundle(bundle_dir)
        bundle = bundle_class(bundle_dir)

        l = new_library(rc.library(args.library))

        if l.get(bundle.identity.vid)  and not args.force:
            prt("{} Bundle is already in library", bundle.identity.name)
            return
        elif bundle.is_built and not args.force and not args.clean:
            prt("{} Bundle is already built",bundle.identity.name)
            return
        else:

            if args.dryrun:
                prt("{} Would build but in dry run ", bundle.identity.name)
                return

            repo.bundle = bundle
             
            if args.clean: 
                bundle.clean()
                
            # Re-create after cleaning is important for something ... 

            bundle = bundle_class(bundle_dir)
                

            prt("{} Building ", bundle.identity.name)

            if not bundle.run_prepare():
                err("{} Prepare failed", bundle.identity.name)
            
            if not bundle.run_build():
                err("{} Build failed", bundle.identity.name)
            
        if args.install and not args.dryrun:
            if not bundle.run_install(force=True):
                err('{} Install failed', bundle.identity.name)
            

    build_dirs = {}
    
    # Find all of the dependencies for the named bundle, and make those first. 
    for root, _, files in os.walk(rc.sourcerepo.dir):
        if 'bundle.yaml' in files:
            bundle_class = load_bundle(root)
            bundle = bundle_class(root)      
            build_dirs[bundle.identity.name] = root 


    if name:
        deps = repo.bundle_deps(name)
        deps.append(name)
        
    else:

        deps = []

        # Walk the subdirectory for the files to build, and
        # add all of their dependencies
        for root, _, files in os.walk(dir_):
            if 'bundle.yaml' in files:
                
                bundle_class = load_bundle(root)
                bundle = bundle_class(root)    

                for dep in repo.bundle_deps(bundle.identity.name):
                    if dep not in deps:
                        deps.append(dep)
                        
                deps.append(bundle.identity.name)
    

    for n in deps:
        try:
            dir_ = build_dirs[n]
        except KeyError:
            err("Failed to find directory for bundle {}".format(n))

        prt('')
        prt("{} Building in {}".format(n, dir_))
        build(dir_)

            
def source_run(args,rc, src):

    from databundles.source.repository.git import GitRepository

    dir_ = args.dir

    if not dir_:
        dir_ = rc.sourcerepo.dir

    for root, _, files in os.walk(dir_):
        if 'bundle.yaml' in files:
            repo = GitRepository(None, root)
            repo.bundle_dir = root
            
            if args.repo_command == 'commit' and repo.needs_commit():
                prt("--- {} {}",args.repo_command, root)
                repo.commit(' '.join(args.message))
                
            elif args.repo_command == 'push' and repo.needs_push():
                prt("--- {} {}",args.repo_command, root)
                repo.push()
                
            elif args.repo_command == 'pull':
                prt("--- {} {}",args.repo_command, root)
                repo.pull()
                
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
    from ..source.repository.git import GitRepository
    
    dir_ = args.dir
    
    if not dir_:
        dir_ = rc.sourcerepo.dir   

    for root, _, files in os.walk(dir_):
        if 'bundle.yaml' in files:

            repo = GitRepository(None, root)
            repo.bundle_dir = root
            if args.commit:
                if repo.needs_commit():
                    print(root)
            elif args.push:
                if repo.needs_push():
                    print(root)
            elif args.init:
                if repo.needs_init():
                    print(root)
            else:
                err("Must specify either --push or --commit")
                
   
         
def source_init(args,rc, src):
    from ..source.repository import new_repository

    dir_ = args.dir
    
    if not dir_:
        dir_ = os.getcwd()
    
    repo = new_repository(rc.sourcerepo(args.name))
    repo.bundle_dir = dir_

    repo.delete_remote()
    import time
    time.sleep(3)
    repo.init()
    repo.init_remote()
    
    repo.push()
    
def source_sync(args,rc, src):
    '''Synchronize all of the repositories with the local library'''
    import databundles.library as library
    from databundles.identity import new_identity

    l = library.new_library(rc.library(args.library))

   
    for repo in rc.sourcerepo.list:
        
        prt('--- Sync with upstream source repository {}', repo.service.ident)
        for e in repo.service.list():

            ident = new_identity(e)

            l.database.add_file(e['clone_url'], repo.service.ident, ident.id_, 
                                state='synced', type_='source', source_url = e['clone_url'], data=e)
            
            prt("Added {:15s} {}",ident.id_,e['clone_url'] )


def source_deps(args,rc, src):
    """Produce a list of dependencies for all of the source bundles"""

    from ..util import toposort
    from ..source.repository import new_repository

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

        graph = toposort(repo.dependencies)
    
        for i,level in enumerate(graph):
            for j, name in enumerate(level):
                print "{:3d} {:3d} {}".format(i,j,name)
            
             
            