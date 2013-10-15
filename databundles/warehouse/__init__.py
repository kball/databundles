
from __future__ import absolute_import
from ..dbexceptions import ConfigurationError
from ..library import LibraryDb, Library
from ..cache import new_cache, CacheInterface
from ..database import new_database



class NullCache(CacheInterface):
    def has(self, rel_path, md5=None, use_upstream=True):
        return False
    

def new_warehouse(config):

    service = config['service'] if 'service' in config else 'relational'
    
    database = new_database(config['database'],'warehouse')
    storage = new_cache(config['storage']) if 'storage' in config else None
    library_database = LibraryDb(**config['library']) if 'library' in config else  LibraryDb(**config['database'])

    library =  Library(cache = NullCache(), 
                 database = library_database, 
                 remote = None)


    if service == 'bigquery':
        pass
    elif service == 'redshift':
        from .redshift import RedshiftWarehouse  #@UnresolvedImport

        return RedshiftWarehouse(database,storage=storage, library=library)
    
    elif service == 'postgres':
        from .postgres import PostgresWarehouse  #@UnresolvedImport

        return PostgresWarehouse(database=database,storage=storage, library=library)
    else:
        from .relational import RelationalWarehouse #@UnresolvedImport
        return RelationalWarehouse(database,storage=storage, library=library)
        
   
    
class ResolverInterface(object):   
    
    def get(self, name):
        raise NotImplemented()
    
    def get_ref(self, name):
        raise NotImplemented()
    
    def url(self, name):
        raise NotImplemented()
    
    
class WarehouseInterface(object):
    
    def __init__(self, database,  library=None, storage=None, resolver = None, logger=None):
        
        self.database = database
        self.storage = storage
        self.library = library
        self.resolver = resolver if resolver else lambda name: False
        self.logger = logger if logger else NullLogger()

        if not self.library:
            self.library = self.database
            
class NullLogger(object):
    
    def __init__(self):
        pass

    def progress(self,type_,name, n, message=None):
        pass
        
    def log(self,message):
        pass
        
    def error(self,message):
        pass 