"""git repository service

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import RepositoryInterface, RepositoryException  #@UnresolvedImport


from sh import git #@UnresolvedImport

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

    def has(self, path):
        pass
        
        
    def add(self,path):
        
        o = git.add(path)
        
        if o.exit_code != 0:
            raise RepositoryException("Failed to add file {} to  git repo: {}".format(path, o))
        
        return True        
  
    def commit(self,message="."):
        
        o = git.commit(a=True, m=message)
        
        if o.exit_code != 0:
            raise RepositoryException("Failed to commit git repo: {}".format(o))
        
        return True  
     

class GitRepository(RepositoryInterface):
    '''
    classdocs
    '''

    def __init__(self,service, dir, **kwargs):
        
        self.service = service
        self.dir_ = dir
        self.impl = GitShellService(self.dir_)

    def ident(self):
        '''Return an identifier for this service'''
        
    def init(self):
        '''Initialize the repository, both load and the upstream'''
        self.impl.init()
    
    def is_initialized(self):
        '''Return true if this repository has already been initialized'''
    
    
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
    
    def push(self):
        '''Push any changes to the repository to the origin server'''
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
    
        