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

                
   
         
def source_list(args,rc, src, names=None):
    '''List all of the source packages'''
    from collections import defaultdict
    import library
    
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


    
def source_make(args,rc, src):

    from databundles.identity import Identity
    
    if args.dir:
        if os.path.exists(args.dir):
            dir_ = args.dir
            name = None
        else:
            try: 
                Identity.parse_name(args.dir)

                name = args.dir
            except:  
                err("Argument '{}' must be either a bundle name or a directory")
            
        
    if not dir_:
        dir_ = rc.sourcerepo.dir
        
    
        
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
        for root, _, files in os.walk(dir_):
            if 'bundle.yaml' in files:
                bundle_class = load_bundle(root)
                bundle = bundle_class(root)      
                build_dirs[bundle.identity.name] = root 
                
        for n in deps:
            dir_ = build_dirs[n]
            prt("{:50s} {}".format(n, dir_))
            build(dir_)
    
        
    else:
        for root, _, files in os.walk(dir):
            if 'bundle.yaml' in files:
                build(root)


def source_run(args,rc, src):

    from source.repository.git import GitRepository

    dir_ = args.dir

    if not dir:
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
            else:
                err("Must specify either --push or --commit")
                
   
         
def source_init(args,rc, src):
    from source.repository import new_repository

    dir_ = args.dir
    
    if not dir:
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
    import library
    from identity import new_identity

    l = library.new_library(rc.library(args.library))

   
    for repo in rc.sourcerepo.list:
        
        prt('--- Sync with upstream source repository {}', repo.service.ident)
        for e in repo.service.list():

            ident = new_identity(e)

            l.database.add_file(e['clone_url'], repo.service.ident, ident.id_, state='synced', type_='source', data=e)
            
            prt("Added {:15s} {}",ident.id_,e['clone_url'] )


def source_deps(args,rc, src):
    """Produce a list of dependencies for all of the source bundles"""

    from util import toposort
    from source.repository import new_repository

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
            
             
            