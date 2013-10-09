"""git repository service

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import RepositoryInterface, RepositoryException  #@UnresolvedImport
from databundles.dbexceptions import ConfigurationError

from sh import git,ErrorReturnCode_1, ErrorReturnCode_128 #@UnresolvedImport


from databundles.util import get_logger

import logging

logger = get_logger(__name__)
logger.setLevel(logging.DEBUG)

class GitShellService(object):
    '''Interact with GIT services using the shell commands'''

    def __init__(self,repo, dir):
        import os
        self.repo = repo
        self.dir_ = dir

        if self.dir_:
            self.saved_path = os.getcwd()
            os.chdir(self.dir_)
        else:
            self.saved_path = None

    def __del__( self ): # Should be ContextManager, but not right model ... 
        import os
        if self.saved_path:
            os.chdir( self.save_path )
        
    def init(self):
        o = git.init()
        
        if o.exit_code != 0:
            raise RepositoryException("Failed to init git repo: {}".format(o))
        
        return True

    def init_remote(self, url):
        
        return git.remote('add','origin',url)


    def deinit(self):
        import os
        fn = os.path.join(self.dir_, '.gitignore')
        if os.path.exists(fn):
            os.remove(fn)
            
        dn = os.path.join(self.dir_, '.git')
        if os.path.exists(dn):
            from  databundles.util import rm_rf
            rm_rf(dn)
            
    def has(self, path):
        pass
        
        
    def add(self,path):
        
        o = git.add(path)
        
        if o.exit_code != 0:
            raise RepositoryException("Failed to add file {} to  git repo: {}".format(path, o))
        
        return True        
  
    def commit(self,message="."):
        
        try:
            o = git.commit(a=True, m=message)
        except ErrorReturnCode_1:
            pass

        return True  
     
    def needs_commit(self):
        import os
        
        try:
            for line in git.status(porcelain=True):
                if line.strip():
                    return True
      
            return False
        except ErrorReturnCode_128 as e:
            logger.error("Needs_commit failed in {}".format(os.getcwd()))
            return False
    
    def needs_push(self):
        import os
        
        try:
            for line in git.push('origin','master',n=True, porcelain=True):
                if '[up to date]' in line:
                    return False
    
            return True
            
        except ErrorReturnCode_128 as e:
            logger.error("Needs_push failed in {}".format(os.getcwd()))
            return False
           
    def ignore(self, pattern):
        import os
        
        fn = os.path.join(self.dir_,'.gitignore')
        
        if os.path.exists(fn):
            with open(fn,'rb') as f:
                lines = set([line.strip() for line in f])
        else:
            lines = set()
            
        lines.add(pattern)
        
        with open(fn,'wb') as f:
            for line in lines:
                f.write(line+'\n')      

    def char_to_line(self,line_proc):
        
        import StringIO
        sio = StringIO.StringIO('bingo')
        def _rcv(chr,stdin):
            sio.write(chr)
            if chr == '\n' or chr == ':':
                # This is a total hack, but there is no other way to detect when the line is
                # done being displayed that looking for the last character, which is not a \n
                if not sio.getvalue().endswith('http:') and not sio.getvalue().endswith('https:'):
                    line_proc(sio.getvalue(),stdin)
                    sio.truncate(0)
        return _rcv
         
            
    def push(self, username="Noone", password="None"):
        '''Push to  remote'''
        import sys, os
        from sh import ErrorReturnCode_128 #@UnresolvedImport

        def line_proc(line,stdin):

            if "Username for" in line:
                stdin.put(username+ "\n")
                
            elif "Password for" in line:
                stdin.put(password+ "\n")

            else:
                print "git-push: ", line.strip()

        rcv = self.char_to_line(line_proc)

        
        try:
            # This is a super hack. See http://amoffat.github.io/sh/tutorials/2-interacting_with_processes.html
            # for some explaination. 
            p =  git.push('-u','origin','master',  _out=rcv,  _out_bufsize=0, _tty_in=True)
            p.exit_code
        except ErrorReturnCode_128:
            raise Exception("""Push to repository repository failed. You will need to store or cache credentials. 
            You can do this by using ssh, .netrc, or a credential maanger. 
            See: https://www.kernel.org/pub/software/scm/git/docs/gitcredentials.html""")
            
        return True

    def pull(self, username="Noone", password="None"):
        '''pull to  remote'''
        import sys, os
        from sh import ErrorReturnCode_128 #@UnresolvedImport

        def line_proc(line,stdin):

            if "Username for" in line:
                stdin.put(username+ "\n")
                
            elif "Password for" in line:
                stdin.put(password+ "\n")

            else:
                print "git-push: ", line.strip()

        rcv = self.char_to_line(line_proc)

        
        try:
            # This is a super hack. See http://amoffat.github.io/sh/tutorials/2-interacting_with_processes.html
            # for some explaination. 
            p =  git.pull(  _out=rcv,  _out_bufsize=0, _tty_in=True)
            p.exit_code
        except ErrorReturnCode_128:
            raise Exception("""Push to repository repository failed. You will need to store or cache credentials. 
            You can do this by using ssh, .netrc, or a credential maanger. 
            See: https://www.kernel.org/pub/software/scm/git/docs/gitcredentials.html""")
            
        return True

    def clone(self,url,  dir_):
        import os
        from databundles.dbexceptions import ConflictError
       
        if not os.path.exists(dir_):
            p = git.clone(url,dir_)
        else:
            raise ConflictError("{} already exists".format(dir_))
        


class GitRepository(RepositoryInterface):
    '''
    classdocs
    '''

    SUFFIX = '-dbundle'

    def __init__(self,service, dir, **kwargs):
        
        self.service = service
        self.dir_ = dir
        self._bundle = None
        self._bundle_dir = None
        self._impl = None
        
        self._dependencies = None
        
    
    @property
    def dir(self):
        return self.dir_
    
    def source_path(self, ident):
        '''Return the absolute directory for a bundle based on its identity'''
        import os
        return os.path.join(self.dir, ident.source_path)
    
    
    def get_bundle(self, ident):
        '''Return a build bundle from the identity'''
        
    
    @property
    def bundle(self):
        if not self._bundle:
            from databundles.dbexceptions import ConfigurationError
            raise ConfigurationError("Must assign bundle to repostitory before this operation")        
        
        return self._bundle
    
    @bundle.setter
    def bundle(self, b):
        from databundles.bundle import BuildBundle
        
        self._bundle = b
    
        if not isinstance(b, BuildBundle):
            raise ValueError("B parameter must be a build bundle ")
        
    
        self._impl = GitShellService(self,b.bundle_dir)

    
    @property
    def bundle_dir(self):
        if not self._bundle and not self._bundle_dir:
            from databundles.dbexceptions import ConfigurationError
            raise ConfigurationError("Must assign bundle or bundle_dir to repostitory before this operation")             
    
        if self._bundle_dir:
            return self._bundle_dir
        else:
            return self.bundle_dir
        
    @bundle_dir.setter
    def bundle_dir(self, bundle_dir):
        self._bundle_dir = bundle_dir
        
        # Import the bundle file from the directory
        from databundles.run import import_file
        import imp, os
        rp = os.path.realpath(os.path.join(bundle_dir, 'bundle.py'))
        mod = import_file(rp)
     
        dir_ = os.path.dirname(rp)
        self.bundle = mod.Bundle(dir_)
        

    @property
    def bundle_ident(self):
        if not self._bundle:
            from databundles.dbexceptions import ConfigurationError
            raise ConfigurationError("Must assign bundle or bundle_dir to repostitory before this operation")             
    
        return self._bundle.identity
        
    @bundle_ident.setter
    def bundle_ident(self, ident):
        self.bundle_dir = self.source_path(ident)

    @property
    def impl(self):
        if not self._impl:
            raise ConfigurationError("Must assign bundle to repostitory before this operation")

        return self._impl

    @property
    def ident(self):
        '''Return an identifier for this service'''
        return self.service.ident
        
    def init(self):
        '''Initialize the repository, both load and the upstream'''
        import os 
        
        self.impl.deinit()
        
        self.bundle.log("Create .git directory")
        self.impl.init()
        
        self.bundle.log("Create .gitignore")
        for p in ('*.pyc', 'build','.project','.pydevproject', 'meta/schema-revised.csv'):
            self.impl.ignore(p)
               
        self.bundle.log("Create remote {}".format(self.name))
   
        self.add('bundle.py')
        self.add('bundle.yaml')

        if os.path.exists(self.bundle.filesystem.path('meta')):
            self.add('meta/*')
     
        if os.path.exists(self.bundle.filesystem.path('config')):
            self.add('config/*')
            
        self.add('.gitignore')
        
        self.commit('Initial commit')
            
        
    def init_remote(self):
        self.bundle.log("Check existence of repository: {}".format(self.name))
        
        if not self.service.has(self.name):
            pass
            #raise ConfigurationError("Repo {} already exists. Checkout instead?".format(self.name))
            self.bundle.log("Creating repository: {}".format(self.name))
            self.service.create(self.name)
         

        self.impl.init_remote(self.service.repo_url(self.name))

    def delete_remote(self):
        
        if  self.service.has(self.name):
            self.bundle.log("Deleting remote: {}".format(self.name))
            self.service.delete(self.name)

        
    def de_init(self):
        self.impl.deinit()
        
    
    def is_initialized(self):
        '''Return true if this repository has already been initialized'''
    
    @property
    def name(self):
        return self.bundle.identity.name+self.SUFFIX
    
    
    def create_upstream(self): raise NotImplemented()
    
    def add(self, path):
        '''Add a file to the repository'''
        return self.impl.add(path)
    
    def commit(self, message):
        return self.impl.commit(message=message)
    
    def needs_commit(self):
        return self.impl.needs_commit()
    
    def needs_push(self):
        return self.impl.needs_push()
    
    def clone(self, url, path, dir_):
        '''Locate the source for the named bundle from the library and retrieve the 
        source '''
        import os
        from urlparse import urlparse

        d = os.path.join(self.dir, path)
       
        impl = GitShellService(self,None)

        impl.clone(url,d)
        
        return d
        
    def push(self, username="Noone", password="None"):
        '''Push any changes to the repository to the origin server'''
        self.bundle.log("Push to remote: {}".format(self.name))
        return self.impl.push(username=username, password=password)
    
    def pull(self, username="Noone", password="None"):
        '''Push any changes to the repository to the origin server'''
        self.bundle.log("Pull from remote: {}".format(self.name))
        return self.impl.pull(username=username, password=password)
    
    def register(self, library): 
        '''Register the source location with the library, and the library
        upstream'''
        raise NotImplemented()
    
    def ignore(self, path):  
        '''Ignore a file'''
        raise NotImplemented()
    

    @property
    def dependencies(self):
        '''Return a set of dependencies for the source packages'''
        from collections import defaultdict
        import os
        from databundles.identity import Identity
        from databundles.util import toposort
        from databundles.run import import_file
        
        if not self._dependencies:
            
            depset = defaultdict(set)
        
            for root, dirs, files in os.walk(self.dir_):
                if 'bundle.yaml' in files:

                    rp = os.path.realpath(os.path.join(root, 'bundle.py'))
                    mod = import_file(rp)
  
                    bundle = mod.Bundle(root)
                    deps =  bundle.library.dependencies

                    for k,v in deps.items():
                        ident = Identity.parse_name(v) # Remove revision 
                        #print "XXX {:50s} {:30s} {}".format(v, ident.name, ident.to_dict())
                        depset[bundle.identity.name].add(ident.name)            
                    
            self._dependencies = depset
            
        return dict(self._dependencies.items())
        
    def bundle_deps(self,name, reverse=False):
        '''Dependencies for a particular bundle'''
        from databundles.identity import Identity
        
        ident = Identity.parse_name(name)
        name = ident.name
        out = []
        all_deps = self.dependencies

        if reverse:

            first = True
            out = set()
            
            def reverse_set(name):
                o = set()
                for k,v in all_deps.items():
                    if name in v:
                        o.add(k)
                return o
            
            deps = reverse_set(name)
            while len(deps):

                out.update(deps)
                
                next_deps = set()
                for name in deps:
                    next_deps.update(reverse_set(name))

                deps = next_deps

            out = list(out)

                               
        else:
            deps = all_deps[ident.name]
            while len(deps) > 0:
                out += deps
                next_deps = []
                for d in deps:
                    if d in all_deps:
                        next_deps += all_deps[d]
                        
                deps = next_deps
                
        final = []
        
        for n in reversed(out):
            if not n in final:
                final.append(n) 
                
        return final
        
    @property
    def topo_deps(self):
        '''Return the dependencies in topologically sorted groups '''
        


    def __str__(self):
        return "<GitRepository: account={}, dir={}".format(self.service, self.dir_)
    
        