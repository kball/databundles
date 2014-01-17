"""Source repositories

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

def new_repository(config):

    from ..service import new_service, GitServiceMarker #@UnresolvedImport
    
    if 'account' in config:
        service = new_service(config['account'])
    else:
        defaults_config = {'user': None, 'password': None, 'org': None}
        defaults_config.update(config)

        service = new_service(defaults_config)
    
    config['service'] = service
    
    try: del config['account']
    except: pass
    
    if isinstance(service,GitServiceMarker):
        from .git import GitRepository #@UnresolvedImport

        return GitRepository(**config)
    else:
        from databundles.dbexceptions import ConfigurationError
        raise ConfigurationError('Unknown {}'.format(type(service)))       

class RepositoryException(Exception):
    pass

class RepositoryInterface(object):
    
    def ident(self):
        '''Return an identifier for this service'''
        
    def initialize(self):
        '''Initialize the repository, both load and the upstream'''
        raise NotImplemented()
    
    def is_initialized(self):
        '''Return true if this repository has already been initialized'''
    
    
    def create_upstream(self): raise NotImplemented()
    
    def init(self): raise NotImplemented()
        
    
    def commit(self): raise NotImplemented()
    
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


