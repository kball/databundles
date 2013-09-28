"""git repository service

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import RepositoryInterface, RepositoryException  #@UnresolvedImport
from databundles.dbexceptions import ConfigurationError

from sh import git,ErrorReturnCode_1 #@UnresolvedImport

class GitShellService(object):
    '''Interact with GIT services using the shell commands'''

    def __init__(self,dir):
        import os
        self.dir_ = dir
        
        self.savedPath = os.getcwd()
        os.chdir(self.dir_)

    def __del__( self ): # Should be ContextManager, but not right model ... 
        import os
        os.chdir( self.savedPath )
        
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
     
    def ignore(self, pattern):
        import os
        
        fn = os.path.join(self.dir_,'.ignore')
        
        if os.path.exists(fn):
            with open(fn,'rb') as f:
                lines = set([line.strip() for line in f])
        else:
            lines = set()
            
        lines.add(pattern)
        
        with open(fn,'wb') as f:
            for line in lines:
                f.write(line+'\n')      
    
    def output(self,line):
        print 'OUTPUT: ', line
     

class GitRepository(RepositoryInterface):
    '''
    classdocs
    '''

    def __init__(self,service, dir, **kwargs):
        
        self.service = service
        self.dir_ = dir
        self._bundle = None
        self._impl = None
        
    
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
        
    
        self._impl = GitShellService(b.bundle_dir)

    @property
    def impl(self):
        if not self._impl:
            raise ConfigurationError("Must assign bundle to repostitory before this operation")

        return self._impl

    def ident(self):
        '''Return an identifier for this service'''
        
    def init(self):
        '''Initialize the repository, both load and the upstream'''
        
        self.bundle.log("Create .git directory")
        self.impl.init()
        
        self.bundle.log("Create .gitignore")
        for p in ('*.pyc', 'build','.project','.pydevproject', 'meta/schema-revised.csv'):
            self.impl.ignore(p)
               
        self.bundle.log("Create remote {}".format(self.name))
   
        self.add('bundle.py')
        self.add('bundle.yaml')
        self.add('meta/*')
        
        self.commit()
            
        
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
        return self.bundle.identity.name+"-dbundle"
    
    
    def create_upstream(self): raise NotImplemented()
    
    def add(self, path):
        '''Add a file to the repository'''
        return self.impl.add(path)
    
    def commit(self):
        return self.impl.commit()
    
    def clone(self, library, name):
        '''Locate the source for the named bundle from the library and retrieve the 
        source '''
        raise NotImplemented()
    
    def register(self, library): 
        '''Register the source location with the library, and the library
        upstream'''
        raise NotImplemented()
    
    def ignore(self, path):  
        '''Ignore a file'''
        raise NotImplemented()

        
    def __str__(self):
        return "<GitRepository: account={}, dir={}".format(self.service, self.dir_)
    
        